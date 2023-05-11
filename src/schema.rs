use std::ops::RangeFrom;

use bonsaidb::core::document::Emit;
use bonsaidb::core::key::time::TimestampAsDays;
use bonsaidb::core::key::Key;
use bonsaidb::core::schema::{Collection, CollectionViewSchema, Schema, View};
use serde::{Deserialize, Serialize};

#[derive(Schema, Debug)]
#[schema(name = "crabtrics", collections = [PodcastDownloads])]
pub struct Crabtrics;

#[derive(Debug, Collection, Serialize, Deserialize)]
#[collection(name = "podcast-downloads", primary_key = EpisodeDateKey, views = [CompleteDownloads, DownloadsByDate])]
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

#[derive(Debug, Hash, Copy, Clone, Eq, PartialEq, Key, Ord, PartialOrd)]
pub struct EpisodeDateKey {
    pub episode: u16,
    pub date: TimestampAsDays,
}

#[derive(Debug, Hash, Copy, Clone, Eq, PartialEq, Key, Ord, PartialOrd)]
pub struct DateEpisodeKey {
    pub date: TimestampAsDays,
    pub episode: u16,
}

impl DateEpisodeKey {
    pub fn range_starting_at(start: TimestampAsDays) -> RangeFrom<DateEpisodeKey> {
        Self {
            date: start,
            episode: 0,
        }..
    }
}

#[derive(Debug, Clone, View)]
#[view(name = "by-date", collection = PodcastDownloads, key = DateEpisodeKey, value = u32)]
pub struct DownloadsByDate;

impl CollectionViewSchema for DownloadsByDate {
    type View = Self;

    fn map(
        &self,
        document: bonsaidb::core::document::CollectionDocument<<Self::View as View>::Collection>,
    ) -> bonsaidb::core::schema::ViewMapResult<'static, Self> {
        document.header.emit_key_and_value(
            DateEpisodeKey {
                date: document.header.id.date,
                episode: document.header.id.episode,
            },
            u32::from(document.contents.full_downloads),
        )
    }
}
