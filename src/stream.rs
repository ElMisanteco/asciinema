use std::future;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use avt::Terminal;
use futures_util::{stream, StreamExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::{io, time};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tracing::info;

use crate::session::{self, Metadata};
use crate::tty::{TtySize, TtyTheme};

pub struct Stream {
    request_tx: mpsc::Sender<Request>,
    request_rx: mpsc::Receiver<Request>,
}

type Request = oneshot::Sender<Subscription>;

struct Subscription {
    init: Event,
    events_rx: broadcast::Receiver<Event>,
}

#[derive(Clone, Copy)]
pub struct EventId(u64);

#[derive(Clone)]
pub enum Event {
    Init(EventId, Duration, TtySize, Option<TtyTheme>, String),
    Output(EventId, Duration, String),
    Input(EventId, Duration, String),
    Resize(EventId, Duration, TtySize),
    Marker(EventId, Duration, String),
    Exit(EventId, Duration, i32),
    Eot(EventId, Duration),
}

#[derive(Clone)]
pub struct Subscriber(mpsc::Sender<Request>);

pub struct LiveStream(mpsc::Sender<session::Event>);

impl Stream {
    pub fn new() -> Self {
        let (request_tx, request_rx) = mpsc::channel(16);

        Stream {
            request_tx,
            request_rx,
        }
    }

    pub fn subscriber(&self) -> Subscriber {
        Subscriber(self.request_tx.clone())
    }

    pub async fn start(self, metadata: &Metadata) -> anyhow::Result<LiveStream> {
        let terminal = Terminal::with_scrollback(
            metadata.term.size.0.into(),
            metadata.term.size.1.into(),
            Some(1000),
        )?;

        let (stream_tx, stream_rx) = mpsc::channel(1024);
        let request_rx = self.request_rx;

        tokio::spawn(run(
            terminal,
            metadata.term.theme.clone(),
            stream_rx,
            request_rx,
        ));

        Ok(LiveStream(stream_tx))
    }
}

async fn run(
    mut terminal: Terminal,
    tty_theme: Option<TtyTheme>,
    mut stream_rx: mpsc::Receiver<session::Event>,
    mut request_rx: mpsc::Receiver<Request>,
) {
    let (broadcast_tx, _) = broadcast::channel(1024);
    let mut stream_time = Duration::from_micros(0);
    let mut last_event_id = EventId::new(0);
    let mut last_event_time = Instant::now();

    loop {
        tokio::select! {
            event = stream_rx.recv() => {
                match event {
                    Some(event) => {
                        last_event_time = Instant::now();
                        last_event_id = last_event_id.next();

                        match event {
                            session::Event::Output(time, text) => {
                                terminal.feed(&text);
                                let _ = broadcast_tx.send(Event::Output(last_event_id, time, text));
                                stream_time = time;
                            }

                            session::Event::Input(time, text) => {
                                let _ = broadcast_tx.send(Event::Input(last_event_id, time, text));
                                stream_time = time;
                            }

                            session::Event::Resize(time, tty_size) => {
                                terminal.resize(tty_size.0.into(), tty_size.1.into());
                                let _ = broadcast_tx.send(Event::Resize(last_event_id, time, tty_size));
                                stream_time = time;
                            }

                            session::Event::Marker(time, label) => {
                                let _ = broadcast_tx.send(Event::Marker(last_event_id, time, label));
                                stream_time = time;
                            }

                            session::Event::Exit(time, status) => {
                                let _ = broadcast_tx.send(Event::Exit(last_event_id, time, status));
                                stream_time = time;
                            }
                        }
                    }

                    None => {
                        let time = stream_time + last_event_time.elapsed();
                        let id = last_event_id.next();
                        let _ = broadcast_tx.send(Event::Eot(id, time));
                        break;
                    },
                }
            }

            request = request_rx.recv() => {
                match request {
                    Some(request) => {
                        let init = if last_event_id.as_u64() > 0 {
                            let elapsed_time = stream_time + last_event_time.elapsed();

                            Event::Init(
                                last_event_id,
                                elapsed_time,
                                terminal.size().into(),
                                tty_theme.clone(),
                                terminal.dump(),
                            )
                        } else {
                            Event::Init(
                                last_event_id,
                                stream_time,
                                terminal.size().into(),
                                tty_theme.clone(),
                                "".to_owned(),
                            )
                        };

                        let events_rx = broadcast_tx.subscribe();
                        let _ = request.send(Subscription { init, events_rx });
                        info!("subscriber count: {}", broadcast_tx.receiver_count());
                    }

                    None => break,
                }
            }
        }
    }
}

impl EventId {
    fn new(id: u64) -> Self {
        EventId(id)
    }

    fn next(&self) -> Self {
        EventId(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<EventId> for u64 {
    fn from(id: EventId) -> Self {
        id.0
    }
}

impl From<u64> for EventId {
    fn from(id: u64) -> Self {
        EventId(id)
    }
}

impl Subscriber {
    pub async fn subscribe(
        &self,
    ) -> anyhow::Result<impl futures_util::Stream<Item = Result<Event, BroadcastStreamRecvError>>>
    {
        let (tx, rx) = oneshot::channel();
        self.0.send(tx).await?;
        let subscription = time::timeout(Duration::from_secs(5), rx).await??;
        let init = stream::once(future::ready(Ok(subscription.init)));
        let events = BroadcastStream::new(subscription.events_rx);

        Ok(init.chain(events))
    }
}

#[async_trait]
impl session::Output for LiveStream {
    async fn event(&mut self, event: session::Event) -> io::Result<()> {
        self.0.send(event).await.map_err(io::Error::other)
    }

    async fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use super::*;
    use crate::session::{Output, TermInfo};

    #[test]
    fn rejects_zero_initial_dimensions() {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.block_on(async {
            for size in [TtySize(0, 3), TtySize(8, 0)] {
                let error = Stream::new().start(&metadata(size)).await.err().unwrap();

                assert_eq!(
                    error.downcast_ref::<avt::InvalidSize>(),
                    Some(&avt::InvalidSize {
                        cols: size.0.into(),
                        rows: size.1.into(),
                    })
                );
            }
        });
    }

    #[test]
    fn late_subscriber_receives_snapshot_and_subsequent_events() {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.block_on(async {
            time::timeout(Duration::from_secs(5), async {
                let stream = Stream::new();
                let subscriber = stream.subscriber();
                let mut live = stream.start(&metadata(TtySize(8, 3))).await.unwrap();
                let mut monitor = subscriber.subscribe().await.unwrap();
                monitor.next().await.unwrap().unwrap();

                let prefix = [
                    session::Event::Output(Duration::from_secs(1), "hello".to_owned()),
                    session::Event::Resize(Duration::from_secs(2), TtySize(12, 4)),
                ];

                for event in prefix {
                    live.event(event).await.unwrap();

                    // Receiving the broadcast ensures the update was applied before subscribing.
                    monitor.next().await.unwrap().unwrap();
                }

                let mut events = subscriber.subscribe().await.unwrap();

                let Event::Init(id, time, size, _, init) = events.next().await.unwrap().unwrap()
                else {
                    panic!("expected late-subscriber initialization");
                };

                assert_eq!(id.as_u64(), 2);
                assert!(time >= Duration::from_secs(2));
                assert_eq!(size, TtySize(12, 4));

                let mut viewer = Terminal::new(size.0.into(), size.1.into()).unwrap();
                viewer.feed(init);

                assert_eq!(viewer.primary_screen().text(), ["hello", "", "", ""]);

                live.event(session::Event::Output(
                    Duration::from_secs(3),
                    "!".to_owned(),
                ))
                .await
                .unwrap();

                let Event::Output(id, time, output) = events.next().await.unwrap().unwrap() else {
                    panic!("expected streamed output");
                };

                assert_eq!(id.as_u64(), 3);
                assert_eq!(time, Duration::from_secs(3));
                assert_eq!(output, "!");
                viewer.feed(output);
                assert_eq!(viewer.primary_screen().text(), ["hello!", "", "", ""]);
            })
            .await
            .expect("stream should deliver initialization and continuation");
        });
    }

    fn metadata(size: TtySize) -> Metadata {
        Metadata {
            time: SystemTime::now(),
            term: TermInfo {
                type_: None,
                version: None,
                size,
                theme: None,
            },
            idle_time_limit: None,
            command: None,
            title: None,
            env: HashMap::new(),
        }
    }
}
