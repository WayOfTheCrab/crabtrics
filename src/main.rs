//! Goals:
//!
//! - Anonymous metrics over time
//! - Count number of full downloads of the podcast

use std::collections::HashMap;
use std::fs::{self, read_dir, File};
use std::io::{BufReader, Read};
use std::net::IpAddr;
use std::path::Path;
use std::time::SystemTime;

use askama::Template;
use bonsaidb::core::document::Emit;
use bonsaidb::core::key::time::TimestampAsDays;
use bonsaidb::core::key::Key;
use bonsaidb::core::schema::{
    Collection, CollectionViewSchema, Schema, SerializedCollection, SerializedView, View,
};
use bonsaidb::core::transaction::{Operation, Transaction};
use bonsaidb::local::config::{Builder, StorageConfiguration};
use bonsaidb::local::Database;
use libflate::gzip::Decoder;
use serde::{Deserialize, Serialize};

use crate::access_logs::LogReader;

mod access_logs;

#[derive(Schema, Debug)]
#[schema(name = "crabtrics", collections = [PodcastDownloads])]
pub struct Crabtrics;

#[derive(Debug, Collection, Serialize, Deserialize)]
#[collection(name = "podcast-downloads", primary_key = EpisodeDateKey, views = [CompleteDownloads])]
pub struct PodcastDownloads {
    pub full_downloads: u16,
    pub partial_downloads: u16,
}

#[derive(Debug, View, Clone, Serialize, Deserialize)]
#[view(name = "complete", key = u16, value = u32, collection = PodcastDownloads)]
pub struct CompleteDownloads;

impl CollectionViewSchema for CompleteDownloads {
    type View = Self;

    fn map(
        &self,
        document: bonsaidb::core::document::CollectionDocument<<Self::View as View>::Collection>,
    ) -> bonsaidb::core::schema::ViewMapResult<'static, Self> {
        document.header.emit_key_and_value(
            document.header.id.episode,
            document.contents.full_downloads as u32,
        )
    }

    fn reduce(
        &self,
        mappings: &[bonsaidb::core::schema::ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> bonsaidb::core::schema::ReduceResult<Self::View> {
        Ok(mappings.iter().map(|mapping| mapping.value).sum())
    }
}

fn main() -> anyhow::Result<()> {
    let (logs_path, episodes_path, reports_path) = if Path::new("stage").exists() {
        (
            Path::new("stage/nginx"),
            Path::new("stage/episodes"),
            Path::new("stage/reports"),
        )
    } else {
        (
            Path::new("/var/log/nginx"),
            Path::new("/home/wotc/episodes"),
            Path::new("/home/wotc/episodes/crabtrics"),
        )
    };

    let mut aggregation = HashMap::new();
    for entry in read_dir(logs_path)? {
        let Ok(entry) = entry else { continue };
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else { continue };
        if file_name.starts_with("access.log") {
            println!("Importing {file_name}");
            let file = BufReader::new(File::open(&entry.path())?);

            if file_name.ends_with(".gz") {
                aggregate_logs(Decoder::new(file)?, &mut aggregation, episodes_path)?;
            } else {
                aggregate_logs(file, &mut aggregation, episodes_path)?;
            }
        }
    }

    let db = Database::open::<Crabtrics>(StorageConfiguration::new("crabtrics.bonsaidb"))?;
    let mut tx = Transaction::new();
    for (key, info) in aggregation {
        let mut partial_downloads = 0;
        let mut full_downloads = 0;
        for visitor in info.bytes_per_requestor.into_values() {
            for (kind, bytes) in visitor {
                if bytes >= *info.sizes.get(&kind).expect("size not computed") {
                    full_downloads += 1;
                } else {
                    partial_downloads += 1;
                }
            }
        }

        tx.push(Operation::overwrite_serialized::<PodcastDownloads, _>(
            &key,
            &PodcastDownloads {
                full_downloads,
                partial_downloads,
            },
        )?);
    }
    tx.apply(&db)?;

    generate_report(&db, reports_path)
}

use interner::global::{GlobalPool, GlobalString};

static STRINGS: GlobalPool<String> = GlobalPool::new();

#[derive(Debug, Hash, Copy, Clone, Eq, PartialEq, Key, Ord, PartialOrd)]
pub struct EpisodeDateKey {
    episode: u16,
    date: TimestampAsDays,
}

#[derive(Debug, Default)]
struct EpisodeDownloads {
    bytes_per_requestor: HashMap<IpAddr, HashMap<GlobalString, u32>>,
    sizes: HashMap<GlobalString, u32>,
}

fn aggregate_logs<R: Read>(
    source: R,
    aggregation: &mut HashMap<EpisodeDateKey, EpisodeDownloads>,
    episodes_path: &Path,
) -> anyhow::Result<()> {
    let mut logs = LogReader::new(source);
    while let Some(log) = logs.read_one()? {
        // Filter errors.
        if log.response_code < 200 || log.response_code > 299 || log.method != "GET" {
            continue;
        }
        // Find files matching /episode-{number}.{extension}.
        let Some(file) = log.path.strip_prefix("/episode-") else { continue };
        let Some((episode, extension)) = file.split_once('.') else { continue };
        assert_eq!(extension, "m4a", "need to support counting sizes by type");
        let Ok(episode): Result<u16, _> = episode.parse() else { continue };

        let episode_downloads = aggregation
            .entry(EpisodeDateKey {
                episode,
                date: TimestampAsDays::try_from(SystemTime::from(log.time))?,
            })
            .or_default();

        let extension = STRINGS.get(extension);
        // Lookup the file size to be able to compute complete downloads.
        if !episode_downloads.sizes.contains_key(&extension) {
            let stat = fs::metadata(episodes_path.join(&log.path[1..]))?;
            episode_downloads
                .sizes
                .insert(extension.clone(), stat.len().try_into()?);
        }

        *episode_downloads
            .bytes_per_requestor
            .entry(log.requestor)
            .or_default()
            .entry(extension)
            .or_default() += log.bytes_sent;
    }
    Ok(())
}

#[derive(Debug, Serialize, Template)]
#[template(path = "index.html")]
struct Report {
    episode_downloads: Vec<EpisodeReport>,
}

#[derive(Debug, Serialize)]
struct EpisodeReport {
    number: u16,
    downloads: u32,
}

fn generate_report(db: &Database, export_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir(export_dir)?;
    let mut csv = csv::Writer::from_path(export_dir.join("downloads.csv"))?;
    csv.write_record(["date", "episode", "full", "partial"])?;
    for dl in PodcastDownloads::all(db).query()? {
        let date = time::OffsetDateTime::from(SystemTime::try_from(dl.header.id.date)?);
        let date = format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day());
        csv.write_record([
            &date,
            &dl.header.id.episode.to_string(),
            &dl.contents.full_downloads.to_string(),
            &dl.contents.partial_downloads.to_string(),
        ])?;
    }
    csv.flush()?;
    drop(csv);

    let mut episode_downloads = Vec::new();
    for mapping in CompleteDownloads::entries(db).reduce_grouped()? {
        episode_downloads.push(EpisodeReport {
            number: mapping.key,
            downloads: mapping.value,
        });
    }

    let rendered = Report { episode_downloads }.render()?;
    fs::write(export_dir.join("index.html"), rendered.as_bytes())?;
    Ok(())
}
