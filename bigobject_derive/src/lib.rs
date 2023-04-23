use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput};

#[proc_macro_derive(BigObject)]
pub fn derive_big_object(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let field_name: Vec<TokenStream> = match input.data {
        Data::Struct(ref data) => data
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| match &field.ident {
                Some(name) => quote! { #name },
                None => quote! { #index },
            })
            .collect(),
        _ => unimplemented!(),
    };
    let field_num = field_name.len();
    assert!(field_num < 255);
    let field_index: Vec<u8> = (0..(field_num as u8)).collect();
    let expanded = quote! {
        impl #impl_generics bigobject::internal::BigObject for #name #ty_generics #where_clause {
            fn initialize<'a, F: FnOnce() -> &'a mut bigobject::internal::Prefix>(&mut self, prefix: F)
            {
                let mut prefix = prefix();
                prefix.push_field_index();
                #(self.#field_name.initialize(|| {
                    prefix.set_field_index(#field_index);
                    &mut prefix
                });)*
                prefix.pop_field_index();
            }
            fn finalize<'a, F: FnOnce() -> &'a mut bigobject::internal::Prefix>(
                &mut self, prefix: F, batch: &mut bigobject::internal::Batch
            ) {
                let mut prefix = prefix();
                prefix.push_field_index();
                #(self.#field_name.finalize(|| {
                    prefix.set_field_index(#field_index);
                    &mut prefix
                }, batch);)*
                prefix.pop_field_index();
            }
            fn big_clone(&self) -> Self {
                Self {
                    #(#field_name: self.#field_name.big_clone(),)*
                }
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}
