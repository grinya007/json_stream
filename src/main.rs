use futures::Stream;
use pipe::PipeReader;
use reqwest::{Body, Client};
use serde::Serialize;
use std::{
    io::{self, Read},
    pin::Pin,
    task::{Context, Poll},
    thread,
};

// The goal of this small reaserch is to find a way to stream JSON string as it comes out of
// serde_json into a POST request without collecting the entire JSON-string on the client-side

// I couldn't make it work with axum_test::TestServer because the functionality of TestRequest is
// limited and it doesn't support streaming
//
// So, in order to mock a server, start `nc -l 127.0.0.1 8787` in another terminal
// then run the program and check back the output of nc
// To make it quit nicely, paste the following to the nc input: `HTTP/1.1 200 OK` then hit Enter
// two times

// pipe::pipe_buffered behaves weird with buffering, it always reads less bytes then the size of
// the buffer
const BUFFER_SIZE: usize = 64;

// mock data
#[derive(Clone, Serialize)]
struct Foo {
    id: usize,
    name: String,
}

#[derive(Serialize)]
struct Foos {
    foos: Vec<Foo>,
}

// pipe::PipeReader wrapper
struct PipeReaderStream {
    inner: PipeReader,
}

impl PipeReaderStream {
    fn new(inner: PipeReader) -> Self {
        Self { inner }
    }
}

impl Stream for PipeReaderStream {
    type Item = Result<bytes::Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buf = [0; BUFFER_SIZE];

        // since serde writes eagerly to the pipe, the stream is always Poll::Ready to produce next
        // item
        Poll::Ready(match self.inner.read(&mut buf) {
            Ok(n) if n == 0 => None,
            Ok(n) => Some(Ok(buf[..n].to_vec().into())),
            Err(e) => Some(Err(e)),
        })
    }
}

#[tokio::main]
async fn main() {
    let (reader, writer) = pipe::pipe_buffered();
    let reader = PipeReaderStream::new(reader);

    // in the current implementation, the data has to be collected in whole, in my application
    // accumulating rust data in memory should not be an issue as opposed to accumulating JSON
    // using struson for lazy json serialization along with consuming some data generator
    // is whole another chapter
    let data = Foos {
        foos: vec![
            Foo {
                id: 1,
                name: "fooooo".to_string()
            };
            5
        ],
    };

    let write = thread::spawn(move || {
        serde_json::to_writer(writer, &data).unwrap();
    });

    let send = tokio::task::spawn(async move {
        Client::new()
            .post("http://127.0.0.1:8787")
            .header("Content-Type", "application/json")
            // when body is Stream, reqwest adds a header: `transfer-encoding: chunked`
            // and sends sequential chunks of bytes prepended with the size of the chunk
            // https://en.wikipedia.org/wiki/Chunked_transfer_encoding
            .body(Body::wrap_stream(reader))
            .send()
            .await
    });

    // wait for serde to finish writing
    write.join().unwrap();
    // wait for reqwest to finish sending and unwrap the server response
    send.await.unwrap().unwrap();
}
