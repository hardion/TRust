//! Runtime-neutral helpers for the client: tokio natively, the event loop of the browser on
//! wasm32, where tokio has no timer and threads do not exist.

use std::future::Future;
use std::time::Duration;

pub use futures_util::future::AbortHandle;
/// `std::time::Instant`, also in a web page.
pub use web_time::Instant;

/// Runs `f` in the background; the handle stops it.
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn(f: impl Future<Output = ()> + Send + 'static) -> AbortHandle {
    let (f, handle) = futures_util::future::abortable(f);
    tokio::spawn(f);
    handle
}

/// Runs `f` in the background; the handle stops it.
#[cfg(target_arch = "wasm32")]
pub fn spawn(f: impl Future<Output = ()> + 'static) -> AbortHandle {
    let (f, handle) = futures_util::future::abortable(f);
    wasm_bindgen_futures::spawn_local(async move {
        let _ = f.await;
    });
    handle
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(d: Duration) {
    tokio::time::sleep(d).await
}

/// `setTimeout` as a future.
#[cfg(target_arch = "wasm32")]
pub async fn sleep(d: Duration) {
    let ms = d.as_millis().min(i32::MAX as u128) as i32;
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let global = js_sys::global();
        if let Ok(set_timeout) = js_sys::Reflect::get(&global, &"setTimeout".into()) {
            let set_timeout: js_sys::Function = set_timeout.into();
            let _ = set_timeout.call2(&global, &resolve, &ms.into());
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

/// The output of `f`, or `None` if it takes longer than `d`.
pub async fn timeout<F: Future>(d: Duration, f: F) -> Option<F::Output> {
    let (f, s) = (std::pin::pin!(f), std::pin::pin!(sleep(d)));
    match futures_util::future::select(f, s).await {
        futures_util::future::Either::Left((v, _)) => Some(v),
        futures_util::future::Either::Right(_) => None,
    }
}
