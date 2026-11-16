use crate::endpoint::ErrorInner;
use crate::endpoint::context::ContextInternalInner;
use restate_sdk_shared_core::{
    AwaitResponse, NotificationHandle, TerminalFailure, UnresolvedFuture, VM, Value,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

pub(crate) struct VmAsyncResultPollFuture {
    state: Option<AsyncResultPollState>,
}

impl VmAsyncResultPollFuture {
    pub fn new(ctx: Arc<Mutex<ContextInternalInner>>, handle: NotificationHandle) -> Self {
        VmAsyncResultPollFuture {
            state: Some(AsyncResultPollState::Init { ctx, handle }),
        }
    }
}

enum AsyncResultPollState {
    Init {
        ctx: Arc<Mutex<ContextInternalInner>>,
        handle: NotificationHandle,
    },
    PollProgress {
        ctx: Arc<Mutex<ContextInternalInner>>,
        handle: NotificationHandle,
    },
    WaitingInput {
        ctx: Arc<Mutex<ContextInternalInner>>,
        handle: NotificationHandle,
    },
}

macro_rules! must_lock {
    ($mutex:expr) => {
        $mutex.try_lock().expect("You're trying to await two futures at the same time and/or trying to perform some operation on the restate context while awaiting a future. This is not supported!")
    };
}

impl Future for VmAsyncResultPollFuture {
    type Output = Result<Value, ErrorInner>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        loop {
            match self
                .state
                .take()
                .expect("Future should not be polled after Poll::Ready")
            {
                AsyncResultPollState::Init { ctx, handle } => {
                    let mut inner_lock = must_lock!(ctx);

                    // Let's consume some output to begin with.
                    // Skip empty buffers: take_output returns an empty buffer when there's
                    // nothing to send (e.g. while replaying completed entries). Sending it
                    // would emit an empty HTTP/2 DATA frame per replayed await, which some
                    // proxies (e.g. Envoy) reject when consecutive. See sdk-rust#114.
                    let b = inner_lock.vm.take_output();
                    if !b.is_empty() && !inner_lock.write.send(b) {
                        return Poll::Ready(Err(ErrorInner::Suspended));
                    }

                    // We can now start polling
                    drop(inner_lock);
                    self.state = Some(AsyncResultPollState::PollProgress { ctx, handle });
                }
                AsyncResultPollState::WaitingInput { ctx, handle } => {
                    let poll_read = must_lock!(ctx).poll_read_input(cx);

                    // Either we got new input, or we're waiting for it.
                    // In both cases, check with the VM first when polled again:
                    // another future might have read the input carrying our notification.
                    self.state = Some(AsyncResultPollState::PollProgress { ctx, handle });
                    if poll_read.is_pending() {
                        return Poll::Pending;
                    }
                }
                AsyncResultPollState::PollProgress { ctx, handle } => {
                    let mut inner_lock = must_lock!(ctx);

                    match inner_lock.vm.do_await(UnresolvedFuture::Single(handle)) {
                        Ok(AwaitResponse::AnyCompleted) => {
                            // We're good, we got the response
                        }
                        Ok(AwaitResponse::WaitingExternalProgress {
                            waiting_input: true,
                            ..
                        }) => {
                            // do_await buffered an AwaitingOnMessage describing what we're
                            // suspending on; flush it to the runtime before we block on input,
                            // otherwise the runtime never learns what we're waiting for.
                            let b = inner_lock.vm.take_output();
                            if !b.is_empty() && !inner_lock.write.send(b) {
                                return Poll::Ready(Err(ErrorInner::Suspended));
                            }
                            drop(inner_lock);
                            self.state = Some(AsyncResultPollState::WaitingInput { ctx, handle });
                            continue;
                        }
                        Ok(AwaitResponse::WaitingExternalProgress { .. })
                        | Ok(AwaitResponse::ExecuteRun(_))
                        | Ok(AwaitResponse::EphemeralNotificationReady(_)) => {
                            // Waiting only on a run proposal is not expected on this await path.
                            // These two are used by the shared-core to implement async run, which is not supported by the rust sdk currently.
                            unimplemented!()
                        }
                        Ok(AwaitResponse::CancelSignalReceived) => {
                            return Poll::Ready(Ok(Value::Failure(TerminalFailure {
                                code: 409,
                                message: "cancelled".to_string(),
                                metadata: vec![],
                            })));
                        }
                        Err(e) => {
                            return Poll::Ready(Err(e.into()));
                        }
                    };

                    // DoProgress might cause a flip of the replaying state
                    inner_lock.maybe_flip_span_replaying_field();

                    // At this point let's try to take the notification
                    match inner_lock.vm.take_notification(handle) {
                        Ok(Some(v)) => return Poll::Ready(Ok(v)),
                        Ok(None) => {
                            panic!(
                                "This is not supposed to happen, handle was flagged as completed"
                            )
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
            }
        }
    }
}
