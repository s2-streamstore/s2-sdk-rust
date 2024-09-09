use std::fmt;

#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(token: String) -> Self {
        Self(token)
    }

    pub fn get(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for SecretString {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(***)")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}
