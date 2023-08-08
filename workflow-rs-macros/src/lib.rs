#![deny(missing_docs, unsafe_code)]
//! # workflow-rs-macros
//!
//! Provides procedural macros for the `workflow-rs` crate.

use std::mem;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote, Attribute, AttributeArgs, Lit, Meta, MetaNameValue, NestedMeta,
    Signature,
};

#[derive(Clone)]
struct MaybeItemFn {
    _attrs: Vec<Attribute>,
    sig: Signature,
    block: TokenStream2,
}

/// This parses a `TokenStream` into a `MaybeItemFn`
/// (just like `ItemFn`, but skips parsing the body).
impl Parse for MaybeItemFn {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let _attrs = input.call(syn::Attribute::parse_outer)?;
        let sig: Signature = input.parse()?;
        let block: TokenStream2 = input.parse()?;
        Ok(Self { _attrs, sig, block })
    }
}

impl ToTokens for MaybeItemFn {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        self.sig.to_tokens(tokens);
        self.block.to_tokens(tokens);
    }
}

/// Turns a function into a workflow task
///
/// The function should be async or return a future.
///
/// The async result must be a `Result<serde_json::Value, E>` type, where `E` is convertible
/// to a `Box<dyn Error + Send + Sync + 'static>`, which is the case for most
/// error types.
///
/// Several options can be provided to the `#[task]` attribute:
///
/// # Name
///
/// ```ignore
/// #[task(task_queue="example")]
///
// / ```
#[proc_macro_attribute]
pub fn task(meta: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(meta as AttributeArgs);
    let mut inner_fn = parse_macro_input!(item as MaybeItemFn);

    // let tas_attributes = Task::from_list(&args).unwrap();

    let name = mem::replace(&mut inner_fn.sig.ident, parse_quote! {inner});
    let name_str = name.to_string();
    // let fq_name = quote! { concat!(module_path!(), "::", #name_str) };

    // let errors: Vec<String> = Vec::new();
    // for arg in &args {
    //     if let Err(e) = interpret_job_arg(&mut options, arg) {
    //         errors.push(e.into_compile_error());
    //     }
    // }

    let queue = args
        .into_iter()
        .flat_map(|arg| match arg {
            NestedMeta::Meta(m) => match (
                m.path()
                    .get_ident()
                    .map(|ident| ident.to_string())
                    .unwrap_or_default()
                    .as_str(),
                m,
            ) {
                (
                    "queue",
                    Meta::NameValue(MetaNameValue {
                        lit: Lit::Str(queue_name),
                        ..
                    }),
                ) => Some(queue_name.value()),
                _ => None,
            },
            _ => None,
        })
        .next()
        .unwrap_or("default".to_owned());

    let expanded = quote! {
        #[allow(non_camel_case_types)]
        #[derive(Copy, Clone)]
        pub struct #name;

        impl #name {
            #inner_fn
        }

        impl workflow_rs::task::TaskFactory for #name {
            fn name(&self) -> &'static str {
                #name_str
            }
            fn queue_name(&self) -> &'static str {
                #queue
            }
            fn builder(&self, input: serde_json::Value) -> workflow_rs::task::Task {
                workflow_rs::task::Task {
                    task_name: #name_str,
                    input,
                    handler: workflow_rs::task::TaskRunFunction(|context, input|{
                        Box::pin(#name::inner(context, input))
                    }),
                }
            }
        }
    };
    // Hand the output tokens back to the compiler.
    TokenStream::from(expanded)
}

/// Turns a function into a workflow
///
/// The function should be async or return a future.
///
/// The async result must be a `Result<serde_json::Value, E>` type, where `E` is convertible
/// to a `Box<dyn Error + Send + Sync + 'static>`, which is the case for most
/// error types.
///
/// Several options can be provided to the `#[workflow]` attribute:
///
/// # Name
///
/// ```ignore
/// #[task(task_queue="example")]
///
// / ```
#[proc_macro_attribute]
pub fn workflow(meta: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(meta as AttributeArgs);
    let mut inner_fn = parse_macro_input!(item as MaybeItemFn);

    // let tas_attributes = Task::from_list(&args).unwrap();

    let name = mem::replace(&mut inner_fn.sig.ident, parse_quote! {inner});
    let name_str = name.to_string();
    // let fq_name = quote! { concat!(module_path!(), "::", #name_str) };

    // let errors: Vec<String> = Vec::new();
    // for arg in &args {
    //     if let Err(e) = interpret_job_arg(&mut options, arg) {
    //         errors.push(e.into_compile_error());
    //     }
    // }

    let queue = args
        .into_iter()
        .flat_map(|arg| match arg {
            NestedMeta::Meta(m) => match (
                m.path()
                    .get_ident()
                    .map(|ident| ident.to_string())
                    .unwrap_or_default()
                    .as_str(),
                m,
            ) {
                (
                    "queue",
                    Meta::NameValue(MetaNameValue {
                        lit: Lit::Str(queue_name),
                        ..
                    }),
                ) => Some(queue_name.value()),
                _ => None,
            },
            _ => None,
        })
        .next()
        .unwrap_or("default".to_owned());

    let expanded = quote! {

        #[allow(non_camel_case_types)]
        #[derive(Copy, Clone)]
        pub struct #name;

        impl #name {
            #inner_fn
        }

        impl workflow_rs::workflow::WorkflowFactory for #name {
            fn name(&self) -> &'static str {
                #name_str
            }
            fn queue_name(&self) -> &'static str {
                #queue
            }
            fn builder(&self, input: Vec<serde_json::Value>) -> workflow_rs::workflow::Workflow {
                workflow_rs::workflow::Workflow{
                    id: #name_str.to_owned(),
                    workflow_name: #name_str,
                    queue: #queue,
                    args: input,
                    handler: workflow_rs::workflow::WorkflowRunFunction(|context, args|{
                        Box::pin(#name::inner(context, args))
                    })
                }
            }
        }
    };
    // Hand the output tokens back to the compiler.
    TokenStream::from(expanded)
}
