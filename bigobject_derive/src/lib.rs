use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput};

#[proc_macro_derive(BigObject)]
pub fn derive_big_object(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let initialize = for_each_field(
        &input.data,
        |index, name| quote! { self.#name.initialize(|| prefix.child(#index)); },
    );
    let finalize = for_each_field(
        &input.data,
        |index, name| quote! { self.#name.finalize(prefix.child(#index), batch); },
    );
    let expanded = quote! {
        impl #impl_generics bigobject::BigObject for #name #ty_generics #where_clause {
            fn initialize<F: FnOnce() -> bigobject::Prefix>(&mut self, prefix: F) {
                #initialize
            }
            fn finalize<F: FnOnce() -> bigobject::Prefix>(&mut self, prefix: F, batch: &mut bigobject::Batch) {
                #finalize
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

fn for_each_field<F: Fn(usize, TokenStream) -> TokenStream>(
    data: &Data,
    generator: F,
) -> TokenStream {
    match *data {
        Data::Struct(ref data) => {
            let inits = data.fields.iter().enumerate().map(|(index, field)| {
                let name = match &field.ident {
                    Some(name) => quote! { #name },
                    None => quote! { #index },
                };
                let generated = generator(index, name);
                quote_spanned!(field.span() => #generated)
            });
            quote! {
                #(#inits;)*
            }
        }
        _ => unimplemented!(),
    }
}
