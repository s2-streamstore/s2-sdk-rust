use std::{collections::HashMap, fs, path::Path};

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream, Result},
    parse_macro_input, Attribute, Data, DeriveInput, Expr, Fields, FieldsNamed, File, Ident, Item,
    ItemEnum, ItemStruct, Lit, Meta,
};

fn find_type_docs(
    source_code: &str,
    target_name: &str,
) -> Option<(Vec<String>, HashMap<String, Vec<String>>)> {
    let syntax: File = syn::parse_file(&source_code).expect("Failed to parse source file");

    for item in syntax.items {
        if let Some(docs) = match item {
            Item::Mod(m) => match m.content {
                Some((_, items)) => find_mod_docs(items, target_name),
                None => None,
            },
            Item::Struct(s) => find_struct_docs(s, target_name.to_string()),
            Item::Enum(e) => find_enum_docs(e, target_name.to_string()),
            _ => None,
        } {
            return Some(docs);
        }
    }

    None
}

fn find_enum_docs(
    ast_enum: ItemEnum,
    target_enum_name: String,
) -> Option<(Vec<String>, HashMap<String, Vec<String>>)> {
    if ast_enum.ident == target_enum_name {
        let enum_docs = extract_doc_strings(&ast_enum.attrs);
        let mut variant_docs = HashMap::new();

        for variant in ast_enum.variants {
            variant_docs.insert(
                variant.ident.to_string(),
                extract_doc_strings(&variant.attrs),
            );

            for field in variant.fields {
                if let Some(field_name) = field.ident.map(|i| i.to_string()) {
                    variant_docs.insert(field_name, extract_doc_strings(&field.attrs));
                }
            }
        }

        return Some((enum_docs, variant_docs));
    }

    None
}

fn find_struct_docs(
    ast_struct: ItemStruct,
    target_struct_name: String,
) -> Option<(Vec<String>, HashMap<String, Vec<String>>)> {
    if ast_struct.ident == target_struct_name {
        let struct_docs = extract_doc_strings(&ast_struct.attrs);

        let field_docs = ast_struct
            .fields
            .iter()
            .filter_map(|field| {
                Some((
                    field.ident.as_ref()?.to_string(),
                    extract_doc_strings(&field.attrs),
                ))
            })
            .collect();

        return Some((struct_docs, field_docs));
    }

    None
}

fn find_mod_docs(
    items: Vec<Item>,
    target_name: &str,
) -> Option<(Vec<String>, HashMap<String, Vec<String>>)> {
    for item in items {
        if let Item::Struct(s) = item.clone() {
            if s.ident == target_name {
                let docs_iter = extract_doc_strings(&s.attrs);

                let field_docs = s
                    .fields
                    .iter()
                    .filter_map(|field| {
                        Some((
                            field.ident.as_ref()?.to_string(),
                            extract_doc_strings(&field.attrs),
                        ))
                    })
                    .collect();

                return Some((docs_iter, field_docs));
            }
        }

        if let Item::Enum(e) = item.clone() {
            if e.ident == target_name {
                let docs_iter = extract_doc_strings(&e.attrs);
                let mut docs = HashMap::new();

                for variant in e.variants {
                    docs.insert(
                        variant.ident.to_string(),
                        extract_doc_strings(&variant.attrs),
                    );

                    for field in variant.fields {
                        if let Some(field_name) = field.ident.map(|i| i.to_string()) {
                            docs.insert(field_name, extract_doc_strings(&field.attrs));
                        }
                    }
                }
                return Some((docs_iter, docs));
            }
        }

        if let Item::Mod(m) = item {
            if let Some((_, items)) = m.content {
                return find_mod_docs(items, target_name);
            }
        }
    }

    None
}

fn extract_doc_strings(attrs: &[Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter_map(|attr| match &attr.meta {
            Meta::NameValue(meta_doc) => {
                let is_doc = meta_doc
                    .path
                    .segments
                    .first()
                    .map(|seg| seg.ident == Ident::new("doc", seg.ident.span()))
                    .unwrap_or(false);

                if is_doc {
                    match &meta_doc.value {
                        Expr::Lit(doc_expr) => {
                            if let Lit::Str(doc_lit) = &doc_expr.lit {
                                Some(doc_lit.value())
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect()
}

struct SyncDocsArgs {
    value: Option<String>,
}

impl Parse for SyncDocsArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        if input.is_empty() {
            return Ok(SyncDocsArgs { value: None });
        }

        let ident: Ident = input.parse()?;
        Ok(SyncDocsArgs {
            value: Some(ident.to_string()),
        })
    }
}

#[proc_macro_attribute]
pub fn sync_docs(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as SyncDocsArgs);
    let mut input_ast = parse_macro_input!(input as DeriveInput);

    let struct_name = if let Some(value) = args.value {
        value
    } else {
        input_ast.ident.to_string()
    };

    // this is to ignore all dervies/cfg etc.
    match &input_ast.data {
        Data::Struct(_) | Data::Enum(_) => {}
        _ => return TokenStream::from(quote! { #input_ast }),
    };

    let source_path =
        Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR is required")).join("s2.v1alpha.rs");

    let raw_source_content = fs::read_to_string(source_path).expect("Failed to read source file");

    if let Some(docs) = find_type_docs(&raw_source_content, &struct_name) {
        for doc in docs.0 {
            input_ast.attrs.push(syn::parse_quote!(#[doc = #doc]));
        }

        if let syn::Data::Struct(ref mut struct_data) = input_ast.data {
            if let Fields::Named(FieldsNamed {
                named: ref mut fields,
                ..
            }) = struct_data.fields
            {
                for field in fields.iter_mut() {
                    let field_name = field.ident.as_ref().unwrap().to_string();
                    if let Some(field_docs) = docs.1.get(&field_name) {
                        for doc in field_docs {
                            field.attrs.push(syn::parse_quote!(#[doc = #doc]));
                        }
                    }
                }
            }
        } else if let syn::Data::Enum(ref mut enum_data) = input_ast.data {
            for variant in &mut enum_data.variants {
                if let Some(variant_docs) = docs.1.get(&variant.ident.to_string()) {
                    for doc in variant_docs {
                        variant.attrs.push(syn::parse_quote!(#[doc = #doc]));
                    }
                }
                for field in variant.fields.iter_mut() {
                    if let Some(field_name) = field.ident.as_ref().map(|i| i.to_string()) {
                        if let Some(field_docs) = docs.1.get(&field_name) {
                            for doc in field_docs {
                                field.attrs.push(syn::parse_quote!(#[doc = #doc]));
                            }
                        }
                    }
                }
            }
        }
    }

    let expanded = quote! {
        #input_ast
    };

    TokenStream::from(expanded)
}
