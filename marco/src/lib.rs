use proc_macro::{TokenStream, TokenTree, Literal};

#[proc_macro]
pub fn to_upper_case_str(streal: TokenStream) -> TokenStream {
    let mut result = String::new();
    for token in streal {
        match token {
            TokenTree::Ident(ident) => {
                result.push_str(&ident.to_string().to_uppercase());
            }
            TokenTree::Punct(punct) => {
                result.push_str(&punct.to_string());
            }
            TokenTree::Literal(literal) => {
                result.push_str(&literal.to_string());
            }
            TokenTree::Group(group) => {
                result.push_str(&group.to_string());
            }
        }
    }
    TokenStream::from(TokenTree::Literal(Literal::string(&result)))
}

#[proc_macro]
pub fn to_lower_case_str(streal: TokenStream) -> TokenStream {
    let mut result = String::new();
    for token in streal {
        match token {
            TokenTree::Ident(ident) => {
                result.push_str(&ident.to_string().to_lowercase());
            }
            TokenTree::Punct(punct) => {
                result.push_str(&punct.to_string());
            }
            TokenTree::Literal(literal) => {
                result.push_str(&literal.to_string());
            }
            TokenTree::Group(group) => {
                result.push_str(&group.to_string());
            }
        }
    }
    TokenStream::from(TokenTree::Literal(Literal::string(&result)))
}
