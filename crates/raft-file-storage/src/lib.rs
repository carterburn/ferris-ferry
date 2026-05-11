use serde::de::DeserializeOwned;
use serde::Serialize;
use std::{mem::size_of, path::PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

use raft_event_loop::types::Storage;

pub struct FileStorage {
    directory: PathBuf,
}

impl FileStorage {
    pub async fn new(directory: PathBuf) -> std::io::Result<Self> {
        fs::create_dir_all(&directory).await?;
        Ok(Self { directory })
    }

    async fn atomic_serialize<T: Serialize>(&self, path: PathBuf, data: T) -> std::io::Result<()> {
        // open temp file
        let tmp_name = format!(
            ".{}.tmp",
            path.file_name()
                .ok_or(std::io::Error::other("Invalid file path"))?
                .to_str()
                .ok_or(std::io::Error::other("Invalid file name"))?
        );
        let tmp_path = path
            .parent()
            .ok_or(std::io::Error::other("Invalid file path"))?
            .join(&tmp_name);
        let mut tmp = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;

        let serialized = postcard::to_allocvec(&data)
            .map_err(|_| std::io::Error::other("Unable to serialize data"))?;
        tmp.write_all(&serialized).await?;
        tmp.sync_all().await?;
        fs::rename(tmp_path, path).await?;

        Ok(())
    }

    async fn read_type<T: DeserializeOwned>(&self, path: PathBuf) -> std::io::Result<Option<T>> {
        let data = match fs::read(&path).await {
            Ok(d) => d,
            Err(e) => {
                use std::io::ErrorKind::*;
                match e.kind() {
                    NotFound => return Ok(None),
                    _ => return Err(e),
                }
            }
        };

        Ok(Some(postcard::from_bytes(&data).map_err(|_| {
            std::io::Error::other("Error deserializing data")
        })?))
    }
}

impl Storage for FileStorage {
    async fn store_metadata(
        &self,
        metadata: raft_event_loop::types::PersistedMetadata,
    ) -> std::io::Result<()> {
        let path = self.directory.join("metadata");
        self.atomic_serialize(path, metadata).await
    }

    async fn restore_metadata(
        &self,
    ) -> std::io::Result<Option<raft_event_loop::types::PersistedMetadata>> {
        let path = self.directory.join("metadata");
        self.read_type(path).await
    }

    async fn store_log_entries(
        &self,
        addendum: raft_event_loop::types::PersistedLogAddendum,
    ) -> std::io::Result<()> {
        // first, find where we should be writing based on start_index (is it overwriting in the
        // log or just append)
        let (mut log, truncate_at) = {
            let log_path = self.directory.join("log");
            let log = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&log_path)
                .await?;
            let mut reader = BufReader::new(log);
            let mut pos = 0;

            loop {
                let Ok(index) = reader.read_u64().await else {
                    // no more frames to read
                    break;
                };

                if index == addendum.start_index {
                    // we should overwrite this entry, so break at pos (hasn't been updated yet)
                    break;
                }

                let Ok(length) = reader.read_u32().await else {
                    // we have a half baked frame so no update to pos
                    break;
                };

                let mut entry = vec![0u8; length as usize];
                if reader.read_exact(&mut entry).await.is_err() {
                    // half baked frame again
                    break;
                }
                // clean read of a frame, advance our position and keep going
                pos += size_of::<u64>() + size_of::<u32>() + length as usize;
            }

            (reader.into_inner(), pos as u64)
        };

        log.seek(std::io::SeekFrom::Start(truncate_at)).await?;
        log.set_len(truncate_at).await?;

        // now we will write serialized entries
        let mut writer = BufWriter::new(log);
        let mut index = addendum.start_index;
        for entry in addendum.entries {
            let Ok(serialized) = postcard::to_allocvec(&entry) else {
                return Err(std::io::Error::other("Unable to serialize log entry"));
            };

            writer.write_u64(index).await?;
            writer.write_u32(serialized.len() as u32).await?;
            writer.write_all(&serialized).await?;
            index += 1;
        }

        writer.flush().await?;
        writer.into_inner().sync_all().await?;

        Ok(())
    }

    async fn restore_log_entries(&self) -> std::io::Result<Option<Vec<raftcore::types::LogEntry>>> {
        let log_path = self.directory.join("log");
        let log = match File::options().read(true).open(&log_path).await {
            Ok(f) => f,
            Err(e) => {
                use std::io::ErrorKind::*;
                match e.kind() {
                    NotFound => return Ok(None),
                    _ => return Err(e),
                }
            }
        };

        let mut reader = BufReader::new(log);

        let mut entries = Vec::new();
        loop {
            let Ok(_index) = reader.read_u64().await else {
                break;
            };
            let Ok(length) = reader.read_u32().await else {
                break;
            };
            let mut buf = vec![0u8; length as usize];
            if reader.read_exact(&mut buf).await.is_err() {
                break;
            }
            let Ok(entry) = postcard::from_bytes(&buf) else {
                return Err(std::io::Error::other("Deserialization error on LogEntry"));
            };
            entries.push(entry);
        }

        Ok(Some(entries))
    }

    async fn store_snapshot(
        &self,
        snapshot: raft_event_loop::types::Snapshot,
    ) -> std::io::Result<()> {
        let path = self.directory.join("snapshot");
        self.atomic_serialize(path, snapshot).await
    }

    async fn restore_snapshot(&self) -> std::io::Result<Option<raft_event_loop::types::Snapshot>> {
        let path = self.directory.join("snapshot");
        self.read_type(path).await
    }

    async fn retrieve_snapshot_bytes(&self) -> std::io::Result<Vec<u8>> {
        let snapshot = self.restore_snapshot().await?;
        let Some(snapshot) = snapshot else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Snapshot not found",
            ));
        };
        Ok(snapshot.data)
    }

    async fn truncate_log(&self) -> std::io::Result<()> {
        let path = self.directory.join("log");
        let f = File::options()
            .write(true)
            .truncate(true)
            .open(&path)
            .await?;
        f.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use raft_event_loop::types::{PersistedLogAddendum, PersistedMetadata, Snapshot, Storage};
    use raftcore::types::LogEntry;
    use tokio::{fs, io::AsyncWriteExt};

    use crate::FileStorage;

    async fn file_exists_in_dir(path: impl AsRef<Path>, filename: &str) -> bool {
        let mut dir = fs::read_dir(path).await.unwrap();

        while let Some(entry) = dir.next_entry().await.unwrap() {
            let name = entry.file_name();
            if name.as_os_str().to_str().unwrap() == filename {
                return true;
            }
        }
        false
    }

    async fn file_does_not_exist_in_dir(path: impl AsRef<Path>, filename: &str) -> bool {
        let mut dir = fs::read_dir(path).await.unwrap();

        while let Some(entry) = dir.next_entry().await.unwrap() {
            let name = entry.file_name();
            if name.as_os_str().to_str().unwrap() == filename {
                return false;
            }
        }
        true
    }

    #[tokio::test]
    async fn test_atomic_serialization() {
        // choosing to use metadata but this mostly tests "atomic_serialize"
        let tmpdir = tempfile::tempdir().unwrap();
        let s = FileStorage::new(tmpdir.path().to_path_buf()).await.unwrap();
        let metadata = PersistedMetadata {
            term: 1,
            voted_for: None,
        };
        s.store_metadata(metadata).await.unwrap();
        // see if "metadata" exists in the tmpdir and .metadata.tmp does not
        assert!(file_exists_in_dir(tmpdir.path(), "metadata").await);
        assert!(file_does_not_exist_in_dir(tmpdir.path(), ".metadata.tmp").await);
        // restore the metadata
        let m = s.restore_metadata().await.unwrap().unwrap();
        assert_eq!(1, m.term);
        assert_eq!(None, m.voted_for);
    }

    #[tokio::test]
    async fn test_entries_rt() {
        let tmpdir = tempfile::tempdir().unwrap();
        let s = FileStorage::new(tmpdir.path().to_path_buf()).await.unwrap();
        let entries = vec![
            LogEntry {
                term: 1,
                command: "CMD1".as_bytes().to_vec(),
            },
            LogEntry {
                term: 1,
                command: "CMD2".as_bytes().to_vec(),
            },
        ];
        s.store_log_entries(raft_event_loop::types::PersistedLogAddendum {
            start_index: 1,
            entries: entries.clone(),
        })
        .await
        .unwrap();

        // check there is a log entries file in dir and that the .log.tmp isn't there
        assert!(file_exists_in_dir(tmpdir.path(), "log").await);
        assert!(file_does_not_exist_in_dir(tmpdir.path(), ".log.tmp").await);
        let l = s.restore_log_entries().await.unwrap().unwrap();
        assert_eq!(l, entries);
    }

    #[tokio::test]
    async fn test_overwrite_entries() {
        // set entries in a log then overwrite them
        let tmpdir = tempfile::tempdir().unwrap();
        let s = FileStorage::new(tmpdir.path().to_path_buf()).await.unwrap();
        let orig_entries = vec![
            LogEntry {
                term: 1,
                command: "CMD1".as_bytes().to_vec(),
            },
            LogEntry {
                term: 1,
                command: "TMPCMD2".as_bytes().to_vec(),
            },
            LogEntry {
                term: 1,
                command: "TMPCMD3".as_bytes().to_vec(),
            },
        ];
        s.store_log_entries(PersistedLogAddendum {
            start_index: 1,
            entries: orig_entries.clone(),
        })
        .await
        .unwrap();

        // now attempt to store 3 new entries at start_index 2
        let new_entries = vec![
            // index 2
            LogEntry {
                term: 1,
                command: "CMD2".as_bytes().to_vec(),
            },
            // index 3
            LogEntry {
                term: 1,
                command: "CMD3".as_bytes().to_vec(),
            },
            // index 4
            LogEntry {
                term: 1,
                command: "CMD4".as_bytes().to_vec(),
            },
        ];
        s.store_log_entries(PersistedLogAddendum {
            start_index: 2,
            entries: new_entries.clone(),
        })
        .await
        .unwrap();

        // get back the entries from the log file and compare
        let log_entries = s.restore_log_entries().await.unwrap().unwrap();

        assert_eq!(log_entries[..1], orig_entries[..1]);
        assert_eq!(log_entries[1..], new_entries[..]);
    }

    #[tokio::test]
    async fn partial_frame() {
        // verify store_log_entries recovers from a partial frame write
        // start by just adding in one entry into the file
        let tmpdir = tempfile::tempdir().unwrap();
        let s = FileStorage::new(tmpdir.path().to_path_buf()).await.unwrap();
        let entries = vec![LogEntry {
            term: 1,
            command: "CMD1".as_bytes().to_vec(),
        }];
        s.store_log_entries(PersistedLogAddendum {
            start_index: 1,
            entries: entries.clone(),
        })
        .await
        .unwrap();
        // then write a partial frame (just an index at 2) and close the file
        {
            let mut f = tokio::fs::File::options()
                .append(true)
                .open(tmpdir.path().to_path_buf().join("log"))
                .await
                .unwrap();
            f.write_u64(2).await.unwrap();
        }
        // add another entry to the log
        let next = vec![LogEntry {
            term: 1,
            command: "CMD2".as_bytes().to_vec(),
        }];
        s.store_log_entries(PersistedLogAddendum {
            start_index: 2,
            entries: next.clone(),
        })
        .await
        .unwrap();

        let log = s.restore_log_entries().await.unwrap().unwrap();
        assert_eq!(log[..1], entries[..1]);
        assert_eq!(log[1..], next[..]);
    }

    #[tokio::test]
    async fn restore_on_empty_dir() {
        // restore files from an empty dir (no writes) to get None
        let tmpdir = tempfile::tempdir().unwrap();
        let s = FileStorage::new(tmpdir.path().to_path_buf()).await.unwrap();
        assert!(s.restore_metadata().await.unwrap().is_none());
        assert!(s.restore_log_entries().await.unwrap().is_none());
        assert!(s.restore_snapshot().await.unwrap().is_none());
        assert!(s.retrieve_snapshot_bytes().await.is_err());
    }

    #[tokio::test]
    async fn truncate_log_restore() {
        // after truncating a log and restore, get an empty vec
        let tmpdir = tempfile::tempdir().unwrap();
        let s = FileStorage::new(tmpdir.path().to_path_buf()).await.unwrap();
        let entries = vec![LogEntry {
            term: 1,
            command: "CMD1".as_bytes().to_vec(),
        }];
        s.store_log_entries(PersistedLogAddendum {
            start_index: 1,
            entries,
        })
        .await
        .unwrap();

        // truncate log
        s.truncate_log().await.unwrap();

        let restored = s.restore_log_entries().await.unwrap().unwrap();
        assert!(restored.is_empty());
    }

    #[tokio::test]
    async fn snapshot_rt() {
        let tmpdir = tempfile::tempdir().unwrap();
        let s = FileStorage::new(tmpdir.path().to_path_buf()).await.unwrap();
        let snapshot = Snapshot {
            last_included_index: 100,
            last_included_term: 2,
            data: "SNAPSHOT".as_bytes().to_vec(),
        };

        s.store_snapshot(snapshot.clone()).await.unwrap();

        // and restore
        let restored = s.restore_snapshot().await.unwrap().unwrap();
        assert_eq!(restored.last_included_index, snapshot.last_included_index);
        assert_eq!(restored.last_included_term, snapshot.last_included_term);
        assert_eq!(restored.data, snapshot.data);
    }
}
