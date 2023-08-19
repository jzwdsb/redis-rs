use proc_macro::{Literal, TokenStream, TokenTree};
extern crate syn;
#[macro_use]
extern crate quote;

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

#[proc_macro_derive(CommandParser)]
pub fn add_command_parser(input: TokenStream) -> TokenStream {

    // Parse the string representation
    let ast = syn::parse(input).unwrap();

    // Build the impl
    let gen = impl_command_parse(&ast);

    // Return the generated impl
    gen
}

fn impl_command_parse(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl CommandParser for #name {
            fn parse(frames: Vec<Frame>) -> Result<Box<dyn CommandApplyer>, RedisErr> {
                Ok(Box::new(#name::from_frames(frames)?))
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(Applyer)]
pub fn add_command_applyer(input: TokenStream) -> TokenStream {

    // Parse the string representation
    let ast = syn::parse(input).unwrap();

    // Build the impl
    let gen = impl_command_apply(&ast);

    // Return the generated impl
    gen
}

fn impl_command_apply(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl CommandApplyer for #name {
            fn apply(self: Box<Self>, db: &mut Database) -> Frame {
                self.apply(db)
            }
        }
    };
    gen.into()
}
