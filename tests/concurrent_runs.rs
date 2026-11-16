//! Regression test for https://github.com/restatedev/sdk-rust/issues/143

use futures::StreamExt;
use futures::channel::mpsc;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use restate_sdk::prelude::*;
use std::convert::Infallible;
use std::time::Duration;

struct Concurrent;

#[service]
impl Concurrent {
    #[handler]
    async fn two_runs(&self, ctx: Context<'_>) -> HandlerResult<()> {
        let runs = [1u32, 2].map(|i| ctx.run(move || async move { Ok(i) }));
        let _: Vec<_> = futures::stream::iter(runs)
            .buffer_unordered(2)
            .collect()
            .await;
        Ok(())
    }
}

/// Encodes a protocol message: 8-byte header (type, length) followed by the protobuf payload.
fn msg(ty: u16, payload: &[u8]) -> Vec<u8> {
    let header = ((ty as u64) << 48) | payload.len() as u64;
    [&header.to_be_bytes()[..], payload].concat()
}

const PROPOSE_RUN_COMPLETION: u16 = 0x0005;

#[tokio::test]
async fn concurrent_runs_complete_when_notifications_arrive_in_one_chunk() {
    let endpoint = Endpoint::builder().bind(Concurrent).build();
    let (input, input_rx) = mpsc::unbounded::<Result<Frame<bytes::Bytes>, Infallible>>();
    let req = http::Request::post("/invoke/Concurrent/two_runs")
        .header("content-type", "application/vnd.restate.invocation.v6")
        .body(StreamBody::new(input_rx))
        .unwrap();
    let mut output = endpoint.handle(req).into_body();

    // StartMessage { id: "x", known_entries: 1 }, InputCommandMessage { value: {} }
    let start = msg(0x0000, &[0x0A, 1, b'x', 0x18, 1]);
    let input_cmd = msg(0x0400, &[0x72, 0]);
    let send = |b: Vec<u8>| input.unbounded_send(Ok(Frame::data(b.into()))).unwrap();
    send([start, input_cmd].concat());

    // Wait until both runs proposed their completion. Every output frame holds whole messages.
    let mut proposals = 0;
    while proposals < 2 {
        let frame = output.frame().await.unwrap().unwrap().into_data().unwrap();
        let mut rest = &frame[..];
        while !rest.is_empty() {
            let len = u32::from_be_bytes(rest[4..8].try_into().unwrap()) as usize;
            proposals += (u16::from_be_bytes([rest[0], rest[1]]) == PROPOSE_RUN_COMPLETION) as u32;
            rest = &rest[8 + len..];
        }
    }

    // Both RunCompletionNotificationMessage { completion_id, value: { content: "1" } } in one chunk
    let run_completion = |id: u8| msg(0x8011, &[0x08, id, 0x2A, 3, 0x0A, 1, b'1']);
    send([run_completion(1), run_completion(2)].concat());

    // The invocation must complete with an EndMessage. Keep the input open: closing it would
    // wake up the parked futures, and the response body only ends after the input is closed.
    let end = msg(0x0003, &[]);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !output
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .ends_with(&end)
        {}
    })
    .await
    .expect("invocation hung");
}
