use std::io::{self, ErrorKind, Read};
use std::net::IpAddr;
use std::str;

use time::format_description::modifier::{
    Day, Hour, Minute, Month, MonthRepr, OffsetHour, OffsetMinute, Second, Year,
};
use time::format_description::Component;
use time::parsing::Parsed;
use time::OffsetDateTime;

#[derive(Debug, Eq, PartialEq)]
pub struct LogEntry<'s> {
    pub requestor: IpAddr,
    pub time: OffsetDateTime,
    pub method: &'s str,
    pub path: &'s str,
    pub response_code: u16,
    pub bytes_sent: u32,
    pub referrer: &'s str,
    pub user_agent: &'s str,
}

pub struct LogReader<R> {
    source: R,
    scratch: Vec<u8>,
}

impl<R> LogReader<R>
where
    R: Read,
{
    pub fn new(source: R) -> Self {
        Self {
            source,
            scratch: Vec::new(),
        }
    }

    pub fn read_one(&mut self) -> anyhow::Result<Option<LogEntry<'_>>> {
        loop {
            self.scratch.clear();

            match self.read_byte() {
                Ok(b'\n') => {
                    // ckip empty lines
                    continue;
                }
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
                Err(err) => anyhow::bail!(err),
            }

            let requestor_end = self.scan_until_slice(b" - ")?;
            let requestor: IpAddr = str::from_utf8(&self.scratch[0..requestor_end])?.parse()?;
            self.scan_until(b'[')?;
            self.scratch.clear();
            let time_end = self.scan_until_slice(b"] \"")?;
            let time = parse_log_date(&self.scratch[..time_end]).unwrap();
            self.scratch.clear();
            let request_end = self.scan_until_slice(b"\" ")?;

            let response_code_start = self.scratch.len();
            let response_code_end = self.scan_until(b' ')?;
            let response_code: u16 =
                str::from_utf8(&self.scratch[response_code_start..response_code_end])?.parse()?;
            let bytes_sent_start = self.scratch.len();
            let bytes_sent_end = self.scan_until_slice(b" \"")?;
            let bytes_sent: u32 =
                str::from_utf8(&self.scratch[bytes_sent_start..bytes_sent_end])?.parse()?;
            let referrer_start = self.scratch.len();
            let referrer_end = self.scan_until_slice(b"\" \"")?;
            let user_agent_start = self.scratch.len();
            let user_agent_end = self.scan_until_slice(b"\"\n")?;

            let request = str::from_utf8(&self.scratch[..request_end])?;
            let (method, path) = if request.is_empty() || response_code == 400 {
                ("", "")
            } else {
                let Some((method, remaining)) = request.split_once(' ') else { anyhow::bail!("invalid http request") };
                let Some((path, _)) = remaining.split_once(' ') else { anyhow::bail!("invalid http request") };
                (method, path)
            };

            return Ok(Some(LogEntry {
                requestor,
                time,
                method,
                path,
                response_code,
                bytes_sent,
                referrer: str::from_utf8(&self.scratch[referrer_start..referrer_end])?,
                user_agent: str::from_utf8(&self.scratch[user_agent_start..user_agent_end])?,
            }));
        }
    }

    fn read_byte(&mut self) -> io::Result<u8> {
        let mut bytes = [0];
        self.source.read_exact(&mut bytes)?;
        self.scratch.push(bytes[0]);
        Ok(bytes[0])
    }

    fn scan_until(&mut self, byte: u8) -> io::Result<usize> {
        loop {
            let read = self.read_byte()?;
            if read == byte {
                return Ok(self.scratch.len() - 1);
            }
        }
    }

    fn scan_until_slice(&mut self, s: &[u8]) -> io::Result<usize> {
        assert!(!s.is_empty());

        'search: loop {
            self.scan_until(s[0])?;
            for byte in &s[1..] {
                let read = self.read_byte()?;
                if *byte != read {
                    continue 'search;
                }
            }

            return Ok(self.scratch.len() - s.len());
        }
    }
}

fn parse_log_date(bytes: &[u8]) -> anyhow::Result<OffsetDateTime> {
    let mut time = Parsed::new();
    let time_bytes = time.parse_component(bytes, Component::Day(Day::default()))?;
    if time_bytes[0] != b'/' {
        anyhow::bail!("missing / after day");
    }
    let mut month = Month::default();
    month.repr = MonthRepr::Short;
    let time_bytes = time.parse_component(&time_bytes[1..], Component::Month(month))?;
    if time_bytes[0] != b'/' {
        anyhow::bail!("missing / after month");
    }
    let time_bytes = time.parse_component(&time_bytes[1..], Component::Year(Year::default()))?;
    if time_bytes[0] != b':' {
        anyhow::bail!("missing : after year");
    }
    let time_bytes = time.parse_component(&time_bytes[1..], Component::Hour(Hour::default()))?;
    if time_bytes[0] != b':' {
        anyhow::bail!("missing : after hour");
    }
    let time_bytes =
        time.parse_component(&time_bytes[1..], Component::Minute(Minute::default()))?;
    if time_bytes[0] != b':' {
        anyhow::bail!("missing : after minute");
    }
    let time_bytes =
        time.parse_component(&time_bytes[1..], Component::Second(Second::default()))?;
    if time_bytes[0] != b' ' {
        anyhow::bail!("missing ` ` after second");
    }
    let time_bytes = time.parse_component(
        &time_bytes[1..],
        Component::OffsetHour(OffsetHour::default()),
    )?;
    let time_bytes =
        time.parse_component(time_bytes, Component::OffsetMinute(OffsetMinute::default()))?;
    if !time_bytes.is_empty() {
        anyhow::bail!("invalid time format: extra trailing data");
    }
    Ok(time.try_into()?)
}

#[test]
fn parsing() {
    use std::net::Ipv4Addr;

    use time::{Date, PrimitiveDateTime, Time};
    const SAMPLE_LOGS: &str = r#"172.56.208.121 - - [08/May/2023:15:08:30 +0000] "GET /episode-001.m4a HTTP/1.1" 206 212698 "https://wayofthecrab.com/" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1"
172.56.208.121 - - [08/May/2023:15:08:30 +0000] "GET /episode-001.m4a HTTP/1.1" 206 303 "https://wayofthecrab.com/" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1"
"#;
    let mut reader = LogReader::new(SAMPLE_LOGS.as_bytes());
    let line_one = reader.read_one().unwrap().unwrap();
    assert_eq!(line_one, LogEntry {
        requestor: IpAddr::V4(Ipv4Addr::new(172, 56, 208, 121)),
        time: PrimitiveDateTime::new(
            Date::from_calendar_date(2023, time::Month::May, 8).unwrap(),
            Time::from_hms(15, 8, 30).unwrap()
        )
        .assume_utc(),
        method: "GET",
        path: "/episode-001.m4a",
        response_code: 206,
        bytes_sent: 212_698,
        referrer: "https://wayofthecrab.com/",
        user_agent: "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1"
    });
    let line_two = reader.read_one().unwrap().unwrap();
    assert_eq!(line_two,
            LogEntry {
                requestor: IpAddr::V4(Ipv4Addr::new(172, 56, 208, 121)),
                time:  PrimitiveDateTime::new(
                    Date::from_calendar_date(2023, time::Month::May, 8).unwrap(),
                    Time::from_hms(15, 8, 30).unwrap()
                )
                .assume_utc(),
                method: "GET",
                path: "/episode-001.m4a",
                response_code: 206,
                bytes_sent: 303,
                referrer: "https://wayofthecrab.com/",
                user_agent: "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1"
            }

    );
    assert!(reader.read_one().unwrap().is_none());
}
