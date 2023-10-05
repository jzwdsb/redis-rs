use proc_macro::{Literal, TokenStream, TokenTree};
use syn::Variant;
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

#[proc_macro_derive(ValueDecorator)]
pub fn add_value_decorator(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    let gen = impl_value_decorator(&ast);

    gen
}

fn impl_value_decorator(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let variants: &syn::punctuated::Punctuated<Variant, syn::token::Comma> = match &ast.data {
        syn::Data::Enum(e) => &e.variants,
        _ => panic!("only enum can be decorated"),
    };
    // for each variant, generate a impl for ValueDecorator
    // impl is_#name, to_#name, as_#name_ref, as_#name_mut
    let mut gen = quote! {};
    for variant in variants {
        let gen_variant = impl_variant(name, variant);
        gen = quote! {
            #gen
            #gen_variant
        };
    }
    let get_type_tokens = impl_value_get_type(name, variants.clone());
    gen = quote! {
        #gen
        #get_type_tokens
    };
    gen.into()
}

fn impl_value_get_type(
    name: &syn::Ident,
    variants: syn::punctuated::Punctuated<Variant, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    let mut gen = quote! {};
    for variant in variants {
        let variant_name = &variant.ident;
        gen = quote! {
            #gen
            #name::#variant_name(_) => ValueType::#variant_name,
        };
    }
    quote! {
        impl #name {
            pub fn get_type(&self) -> ValueType {
                match self {
                    #gen
                }
            }
        }
    }
}

fn impl_variant(name: &syn::Ident, variant: &Variant) -> proc_macro2::TokenStream {
    let variant_name = &variant.ident;
    let lower_name = format!("{}", variant_name).to_lowercase();
    let is_name = syn::Ident::new(&format!("is_{}", lower_name), variant_name.span());
    let to_name = syn::Ident::new(&format!("to_{}", lower_name), variant_name.span());
    let as_name_ref = syn::Ident::new(&format!("as_{}_ref", lower_name), variant_name.span());
    let as_name_mut = syn::Ident::new(&format!("as_{}_mut", lower_name), variant_name.span());
    let inside = match &variant.fields {
        syn::Fields::Unnamed(fields) => {
            let inside = &fields.unnamed.first().unwrap().ty;
            quote! { #inside }
        }
        _ => panic!("only tuple struct is supported"),
    };
    let gen_variant = quote! {
        impl #name {
            #[allow(dead_code)]
            pub fn #is_name(&self) -> bool {
                match self {
                    #name::#variant_name(_) => true,
                    _ => false,
                }
            }

            #[allow(dead_code)]
            pub fn #to_name(self) -> Option<#inside> {
                match self {
                    #name::#variant_name(v) => Some(v),
                    _ => None,
                }
            }

            #[allow(dead_code)]
            pub fn #as_name_ref(&self) -> Option<&#inside> {
                match self {
                    #name::#variant_name(v) => Some(v),
                    _ => None,
                }
            }

            #[allow(dead_code)]
            pub fn #as_name_mut(&mut self) -> Option<&mut #inside> {
                match self {
                    #name::#variant_name(v) => Some(v),
                    _ => None,
                }
            }
        }
    };

    gen_variant
}

#[proc_macro_derive(Getter)]
pub fn add_getter_ref(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    let gen = impl_getter_ref(&ast);

    gen
}

fn impl_getter_ref(ast: &syn::DeriveInput) -> TokenStream {
    let struct_name = &ast.ident;
    let fields = match &ast.data {
        syn::Data::Struct(s) => &s.fields,
        _ => panic!("only struct can be decorated"),
    };

    let mut gen = quote! {};

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;
        let field_name_str = field_name.to_string();
        let get_ref_name = syn::Ident::new(
            format!("get_{}_ref", field_name_str).as_str(),
            field_name.span(),
        );
        let get_name = syn::Ident::new(
            format!("get_{}", field_name_str).as_str(),
            field_name.span(),
        );
        gen = quote! {
            #gen
            pub fn #get_ref_name(&self) -> &#field_type {
                &self.#field_name
            }
            pub fn #get_name(&self) -> #field_type {
                self.#field_name.clone()
            }
        };
    }

    gen = quote! {
        impl #struct_name {
            #gen
        }
    };

    gen.into()
}
