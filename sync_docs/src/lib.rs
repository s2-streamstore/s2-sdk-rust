use std::{collections::HashMap, fs, path::Path};

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream, Result},
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
    Expr, Field, Fields, FieldsNamed, File, Ident, Item, ItemEnum, ItemStruct, Lit, LitStr, Token,
    Variant,
};

/// First value refers to the type docs, second value (hashmap) refers to the field or variant docs.
type CollectedDocs = (Vec<String>, HashMap<String, Vec<String>>);

fn find_type_docs(parsed_file: File, target_name: &str) -> Option<CollectedDocs> {
    for item in parsed_file.items {
        if let Some(docs) = match item {
            Item::Mod(m) => find_mod_docs(m, target_name),
            Item::Struct(s) => find_struct_docs(s, target_name.to_string()),
            Item::Enum(e) => find_enum_docs(e, target_name.to_string()),
            Item::Impl(i) => find_impl_fn_docs(i, target_name),
            Item::Fn(f) => find_fn_docs(f, target_name.to_string()),
            _ => None,
        } {
            return Some(docs);
        }
    }

    None
}

fn find_enum_docs(ast_enum: ItemEnum, target_enum_name: String) -> Option<CollectedDocs> {
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

fn find_struct_docs(ast_struct: ItemStruct, target_struct_name: String) -> Option<CollectedDocs> {
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

fn find_fn_docs(ast_fn: syn::ItemFn, target_fn_name: String) -> Option<CollectedDocs> {
    if ast_fn.sig.ident == target_fn_name {
        let fn_docs = extract_doc_strings(&ast_fn.attrs);
        return Some((fn_docs, HashMap::new()));
    }

    None
}

fn find_impl_fn_docs(impl_block: syn::ItemImpl, target_name: &str) -> Option<CollectedDocs> {
    for item in impl_block.items {
        if let syn::ImplItem::Fn(method) = item {
            if method.sig.ident == target_name {
                let fn_docs = extract_doc_strings(&method.attrs);
                return Some((fn_docs, HashMap::new()));
            }
        }
    }
    None
}

/// Recursively search for the target module or type in the given items.
fn find_mod_docs(mod_ast: syn::ItemMod, target_name: &str) -> Option<CollectedDocs> {
    for item in mod_ast.content?.1 {
        match item {
            Item::Struct(s) => {
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
            Item::Enum(e) => {
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
            Item::Impl(impl_block) => {
                if let Some(docs) = find_impl_fn_docs(impl_block, target_name) {
                    return Some(docs);
                }
            }
            Item::Mod(m) => {
                if let Some(docs) = find_mod_docs(m, target_name) {
                    return Some(docs);
                }
            }
            _ => {}
        }
    }

    None
}

fn extract_doc_strings(attrs: &[syn::Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter_map(|attr| {
            if attr.path().is_ident("doc") {
                if let syn::Meta::NameValue(name_value) = &attr.meta {
                    match &name_value.value {
                        Expr::Lit(doc_expr) => match &doc_expr.lit {
                            Lit::Str(doc_lit) => Some(doc_lit.value()),
                            _ => None,
                        },
                        _ => None,
                    }
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

#[derive(Debug)]
struct KeyValue {
    key: Ident,
    _eq: Token![=],
    value: LitStr,
}

impl Parse for KeyValue {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(KeyValue {
            key: input.parse()?,
            _eq: input.parse()?,
            value: input.parse()?,
        })
    }
}

#[derive(Debug)]
struct RenameArgs {
    mapping: HashMap<String, String>,
}

impl Parse for RenameArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut mapping = HashMap::new();

        while !input.is_empty() {
            let pair = input.parse::<KeyValue>()?;
            mapping.insert(pair.key.to_string(), pair.value.value());
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(RenameArgs { mapping })
    }
}

/// Synchronize the docs from the compiled prost file to the target type.
#[proc_macro_attribute]
pub fn sync_docs(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as RenameArgs);
    let mut input_item = parse_macro_input!(input as Item);

    let source_path = {
        let out_dir = match std::env::var("OUT_DIR") {
            Ok(dir) => dir,
            Err(_) => {
                return syn::Error::new(proc_macro2::Span::call_site(), "OUT_DIR is required")
                    .to_compile_error()
                    .into();
            }
        };

        let prost_file = match std::env::var("COMPILED_PROST_FILE") {
            Ok(file) => file,
            Err(_) => {
                return syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "COMPILED_PROST_FILE is required",
                )
                .to_compile_error()
                .into();
            }
        };

        Path::new(&out_dir).join(prost_file)
    };

    let raw_file_content = match fs::read_to_string(source_path) {
        Ok(content) => content,
        Err(_) => {
            return syn::Error::new(
                proc_macro2::Span::call_site(),
                "Failed to read the compiled prost file",
            )
            .to_compile_error()
            .into();
        }
    };

    let parsed_file = match syn::parse_file(&raw_file_content) {
        Ok(file) => file,
        Err(_) => {
            return syn::Error::new(
                proc_macro2::Span::call_site(),
                "Failed to parse the compiled prost file",
            )
            .to_compile_error()
            .into();
        }
    };

    let (type_name, attrs) = match &mut input_item {
        Item::Struct(s) => (s.ident.to_string(), &mut s.attrs),
        Item::Enum(e) => (e.ident.to_string(), &mut e.attrs),
        Item::Fn(f) => (f.sig.ident.to_string(), &mut f.attrs),
        _ => {
            return syn::Error::new(
                proc_macro2::Span::call_site(),
                "Only structs, enums, and functions are supported",
            )
            .to_compile_error()
            .into();
        }
    };
    let type_name = args.mapping.get(&type_name).unwrap_or(&type_name);

    if let Some((type_docs, field_or_variant_docs)) = find_type_docs(parsed_file, type_name) {
        for doc in type_docs {
            attrs.insert(0, syn::parse_quote!(#[doc = #doc]));
        }

        match &mut input_item {
            Item::Struct(s) => {
                if let Fields::Named(FieldsNamed { named: fields, .. }) = &mut s.fields {
                    update_struct_field_docs(fields, &field_or_variant_docs, &args.mapping);
                }
            }
            Item::Enum(e) => {
                update_enum_variant_docs(&mut e.variants, &field_or_variant_docs, &args.mapping);
            }
            _ => {}
        };
    }

    let expanded = quote! {
        #input_item
    };

    TokenStream::from(expanded)
}

fn update_struct_field_docs(
    fields: &mut Punctuated<Field, Comma>,
    field_docs: &HashMap<String, Vec<String>>,
    field_mapping: &HashMap<String, String>,
) {
    for field in fields {
        if let Some(field_name) = field.ident.as_ref().map(|i| i.to_string()) {
            let field_name = field_mapping.get(&field_name).unwrap_or(&field_name);
            if let Some(docs) = field_docs.get(field_name) {
                field
                    .attrs
                    .extend(docs.iter().map(|doc| syn::parse_quote!(#[doc = #doc])));
            }
        }
    }
}

fn update_enum_variant_docs(
    variants: &mut Punctuated<Variant, Comma>,
    variant_docs: &HashMap<String, Vec<String>>,
    variant_mapping: &HashMap<String, String>,
) {
    for variant in variants {
        let variant_name = variant.ident.to_string();
        let variant_name = variant_mapping.get(&variant_name).unwrap_or(&variant_name);
        if let Some(variant_docs) = variant_docs.get(variant_name) {
            variant.attrs.extend(
                variant_docs
                    .iter()
                    .map(|doc| syn::parse_quote!(#[doc = #doc])),
            );
        }

        for field in &mut variant.fields {
            if let Some(field_name) = field.ident.as_ref().map(|i| i.to_string()) {
                let field_name = variant_mapping.get(&field_name).unwrap_or(&field_name);
                if let Some(field_docs) = variant_docs.get(field_name) {
                    field.attrs.extend(
                        field_docs
                            .iter()
                            .map(|doc| syn::parse_quote!(#[doc = #doc])),
                    );
                }
            }
        }
    }
}
