# Crabtrics

A purpose-built log analyzer for [The Way of the
Crab](https://wayofthecrab.com/).

## Why?

- Wanted to be able to measure listens per episode over time.
- Tried a couple off-the-shelf log analyzers
  - Most were overkill
  - Retained data that may be consdered personal data
  - Didn't give a concise view relavent to a podcast

## How?

WotC is hosted using nginx. This tool analyzes nginx's log files to look for
downloads of the various audio files. Many clients break requests into chunks to
prevent loading the entire file when streaming, so this tool accumulates the
total number of bytes per file per IP address before analyzing if each IP
address counts as a full or partial download.

Data that might be labeled as personal data (such as the combination of
timestamp, IP address, and User Agent) are available in our logs for 14 days.
Older logs are automatically deleted via `logrotate`.

The only data persisted to the database is:

- Per episode and date:
  - full downloads
  - partial downloads

As such, no potentially personal data is archived by our server.
