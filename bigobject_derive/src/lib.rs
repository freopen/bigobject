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
        |index, name| quote! { self.#name.initialize(|| prefix.child(&#index)); },
    );
    let finalize = for_each_field(
        &input.data,
        |index, name| quote! { self.#name.finalize(|| prefix.child(&#index), batch); },
    );
    let clone = for_each_field(&input.data, |_index, name| {
        quote! {
            #name: self.#name.internal_clone(),
        }
    });
    let expanded = quote! {
        impl #impl_generics bigobject::BigObject for #name #ty_generics #where_clause {
            fn initialize<F>(&mut self, prefix: F)
            where
                F: FnOnce() -> bigobject::internal::Prefix,
            {
                let prefix = prefix();
                #initialize
            }
            fn finalize<F>(&mut self, prefix: F, batch: &mut bigobject::internal::Batch)
            where
                F: FnOnce() -> bigobject::internal::Prefix,
            {
                let prefix = prefix();
                #finalize
            }
        }
        impl #impl_generics bigobject::internal::InternalClone for #name #ty_generics #where_clause {
            fn internal_clone(&self) -> #name {
                Self{
                    #clone
                }
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
                #(#inits)*
            }
        }
        _ => unimplemented!(),
    }
}
