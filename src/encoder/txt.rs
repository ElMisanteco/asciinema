use std::io;

use avt::observer::Transcript;
use avt::Terminal;

use crate::asciicast::{Event, EventData, Header};

pub struct TextEncoder {
    state: Option<(Terminal, Transcript)>,
}

impl TextEncoder {
    pub fn new() -> Self {
        TextEncoder { state: None }
    }
}

impl super::Encoder for TextEncoder {
    fn header(&mut self, header: &Header) -> io::Result<Vec<u8>> {
        let terminal =
            Terminal::with_scrollback(header.term_cols.into(), header.term_rows.into(), Some(100))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        self.state = Some((terminal, Transcript::new()));

        Ok(Vec::new())
    }

    fn event(&mut self, event: Event) -> Vec<u8> {
        use EventData::*;

        let (terminal, transcript) = self.state.as_mut().unwrap();

        let lines = match &event.data {
            Output(data) => terminal.feed_with(data, transcript),

            Resize(cols, rows) => terminal.resize_with((*cols).into(), (*rows).into(), transcript),

            _ => return Vec::new(),
        };

        text_lines_to_bytes(lines)
    }

    fn finish(&mut self) -> Vec<u8> {
        let (terminal, transcript) = self.state.take().unwrap();

        text_lines_to_bytes(transcript.finish(&terminal))
    }
}

fn text_lines_to_bytes<S: AsRef<str>>(lines: impl IntoIterator<Item = S>) -> Vec<u8> {
    lines.into_iter().fold(Vec::new(), |mut bytes, line| {
        bytes.extend_from_slice(line.as_ref().as_bytes());
        bytes.push(b'\n');

        bytes
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::TextEncoder;
    use crate::asciicast::{Event, Header};
    use crate::encoder::Encoder;

    #[test]
    fn encoder() {
        let mut enc = TextEncoder::new();

        let header = Header {
            term_cols: 3,
            term_rows: 1,
            ..Default::default()
        };

        assert!(enc.header(&header).unwrap().is_empty());

        assert!(enc
            .event(Event::output(
                Duration::from_micros(0),
                "he\x1b[1mllo\r\n".to_owned()
            ))
            .is_empty());

        assert!(enc
            .event(Event::output(
                Duration::from_micros(1),
                "world\r\n".to_owned()
            ))
            .is_empty());

        assert_eq!(enc.finish(), "hello\nworld\n".as_bytes());
    }

    #[test]
    fn emits_text_from_output_resize_and_finish() {
        let mut enc = TextEncoder::new();

        enc.header(&Header {
            term_cols: 8,
            term_rows: 3,
            ..Default::default()
        })
        .unwrap();

        // Exceed retained history so both output and resize emit text before finish.
        let lines: Vec<_> = (0..110).map(|n| format!("line{n}")).collect();
        let output = enc.event(Event::output(Duration::ZERO, lines.join("\r\n")));
        let resized = enc.event(Event::resize(Duration::from_secs(1), (8, 1)));
        let finished = enc.finish();

        assert!(!output.is_empty());
        assert!(!resized.is_empty());
        assert!(!finished.is_empty());
        assert_eq!(
            [output, resized, finished].concat(),
            format!("{}\n", lines.join("\n")).as_bytes()
        );
    }
}
