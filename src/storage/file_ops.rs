use std::{
    cell::RefCell,
    fmt::Display,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Seek, SeekFrom, Write},
    str::FromStr,
};

use crate::{
    types::{ServerId, Term},
    LogEntry, LogEntryCommand,
};

use log::info;

pub trait RaftFileOps<T: Clone + FromStr + Display> {
    fn read_term_and_voted_for(&self) -> Result<(Term, ServerId), io::Error>;
    fn write_term_and_voted_for(
        &self,
        term: Term,
        voted_for: Option<ServerId>,
    ) -> Result<(), io::Error>;
    fn read_logs(&self, log_index: u64) -> Result<Vec<LogEntry<T>>, io::Error>;
    fn append_logs(&mut self, entries: &Vec<LogEntry<T>>) -> Result<(), io::Error>;
    fn append_logs_at(
        &mut self,
        entries: &Vec<LogEntry<T>>,
        log_index: u64,
    ) -> Result<(), io::Error>;
}

pub struct DirectFileOpsWriter {
    file_path: String,
    file: RefCell<Option<File>>,
}

impl DirectFileOpsWriter {
    pub fn new(file_path: &str, server_id: ServerId) -> Result<Self, io::Error> {
        let file_name = format!("{}_server_{}", file_path, server_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_name)?;

        Ok(DirectFileOpsWriter {
            file_path: file_name,
            file: RefCell::new(Some(file)),
        })
    }
}

impl Drop for DirectFileOpsWriter {
    fn drop(&mut self) {
        match fs::remove_file(&self.file_path) {
            Ok(()) => (),
            Err(e) => {
                panic!("Error removing file: {}", e);
            }
        }
    }
}

impl<T: Clone + FromStr + Display> RaftFileOps<T> for DirectFileOpsWriter {
    fn read_term_and_voted_for(&self) -> Result<(Term, ServerId), io::Error> {
        // let mut file = self.file.as_ref().expect("File not found");
        let binding = self.file.borrow_mut();
        let mut file = binding.as_ref().expect("File not found");
        file.seek(SeekFrom::Start(0))?;
        let mut buf_reader = BufReader::new(file);
        let mut line = String::new();
        buf_reader.read_line(&mut line)?;
        let values: Vec<&str> = line.split(',').collect();
        let term: u64 = values[0]
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let voted_for: u64 = values[1]
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        return Ok((term, voted_for));
    }

    /// This function has been a bit of a pain. The earlier implemenation read the file using
    /// the file param and tried to overwrite the first line but that doesn't seem to work because
    /// a writeln function will literally just replace the bytes - so if the first line was 1,1000
    /// and the replacement is 1,2 this will shift the 3 zeroes to the next line
    /// The solution for now is to read the entire file into memory, modify the content and write it back
    /// There are obviously better ways but this should be good enough for now
    fn write_term_and_voted_for(
        &self,
        term: Term,
        voted_for: Option<ServerId>,
    ) -> Result<(), io::Error> {
        let content = fs::read_to_string(&self.file_path)?;
        let mut lines: Vec<&str> = content.lines().collect();
        let new_line = format!("{},{}\n", term, voted_for.unwrap_or(100000));
        if !lines.is_empty() {
            lines[0] = new_line.as_str();
        } else {
            lines.push(new_line.as_str());
        }
        let new_content = lines.join("\n");

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.file_path)?;
        file.write_all(new_content.as_bytes())?;
        Ok(())
    }

    fn read_logs(&self, log_index: u64) -> Result<Vec<LogEntry<T>>, io::Error> {
        let mut binding = self.file.borrow_mut();
        let file = binding.as_mut().expect("File not found");
        file.seek(SeekFrom::Start(0))?;
        let mut buf_reader = BufReader::new(file);
        let mut line = String::new();
        let mut entries: Vec<LogEntry<T>> = Vec::new();
        for _ in 0..log_index {
            if buf_reader.read_line(&mut line)? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "log_index out of bounds",
                ));
            }
            line.clear();
        }

        for line in buf_reader.lines() {
            let l = line?;
            let values: Vec<&str> = l.split(',').collect();
            let term: Term = values[0].trim().parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid term: {}", e))
            })?;
            let index: u64 = values[1].trim().parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid index: {}", e))
            })?;
            let command: LogEntryCommand = values[2].trim().parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid command: {}", e),
                )
            })?;
            let key: String = values[3].trim().to_string();
            let value: T = values[4].trim().parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid value"))
            })?;
            entries.push(LogEntry {
                term,
                index,
                command,
                key,
                value,
            });
        }
        return Ok(entries);
    }

    fn append_logs(&mut self, entries: &Vec<LogEntry<T>>) -> Result<(), io::Error> {
        let mut binding = self.file.borrow_mut();
        let file = binding.as_mut().expect("File not found");
        file.seek(SeekFrom::End(0))?;
        for entry in entries {
            writeln!(
                file,
                "{},{},{},{},{}",
                entry.term, entry.index, entry.command, entry.key, entry.value
            )?;
        }
        file.flush()?;
        Ok(())
    }

    /**
     * This method is used to truncate the logs starting at log_index+1 and then
     * append the new logs past that point
     * Unfortunately, this function needs to re-open the file because Rust's borrow checker refuses
     * to allow me to use the existing file handle
     */
    fn append_logs_at(
        &mut self,
        entries: &Vec<LogEntry<T>>,
        log_index: u64,
    ) -> Result<(), io::Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buf_reader = BufReader::new(&file);
        let mut line = String::new();
        let mut position = 0;
        let mut lines_read = 0;

        while lines_read <= log_index {
            line.clear();
            let bytes_read = buf_reader.read_line(&mut line)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("log_index {} out of bounds", log_index),
                ));
            }
            position += bytes_read as u64;
            lines_read += 1;
        }

        drop(buf_reader);
        file.set_len(position)?;
        file.seek(SeekFrom::Start(position))?;
        for entry in entries {
            writeln!(
                file,
                "{},{},{},{},{}",
                entry.term, entry.index, entry.command, entry.key, entry.value
            )?;
        }
        file.flush()?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct TestEntryData(String);

impl std::fmt::Display for TestEntryData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TestEntryData {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TestEntryData(s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use log::info;

    use std::{
        fs::remove_file,
        io::{self},
    };

    use crate::{LogEntry, LogEntryCommand};

    use super::{DirectFileOpsWriter, RaftFileOps, TestEntryData};

    #[test]
    fn test_term_and_voted_for_write_and_read() -> Result<(), io::Error> {
        info!("Starting test_term_and_voted_for_write_and_read");
        let _ = env_logger::try_init();
        let temp_file = "temp_file";
        let mut ops = DirectFileOpsWriter::new(temp_file, 0)?;
        <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::write_term_and_voted_for(
            &mut ops,
            1,
            Option::Some(2),
        )?;
        let (term, voted_for) =
            <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::read_term_and_voted_for(&ops)?;
        // remove_file(format!("{}_server_{}", temp_file, 0))?;
        assert_eq!(term, 1);
        assert_eq!(voted_for, 2);
        Ok(())
    }

    #[test]
    fn test_log_entries_write_and_read() -> Result<(), io::Error> {
        info!("Starting test_log_entries_write_and_read");
        let _ = env_logger::try_init();
        let temp_file = "temp_file";
        let mut ops = DirectFileOpsWriter::new(temp_file, 0)?;
        <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::write_term_and_voted_for(
            &mut ops,
            1,
            Option::Some(2),
        )?;

        //  generate a bunch of log entries and write them
        let mut log_entries: Vec<LogEntry<TestEntryData>> = Vec::new();
        let mut log_index: u64 = 1;
        for _ in 0..20 {
            let log_entry: LogEntry<TestEntryData> = LogEntry {
                term: 1,
                index: log_index,
                command: LogEntryCommand::Set,
                key: log_index.to_string(),
                value: TestEntryData(format!("value_{}", log_index)),
            };
            log_entries.push(log_entry);
            log_index += 1;
        }
        ops.append_logs(&log_entries)?;

        //  read your own writes here
        let read_log_entries =
            <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::read_logs(&mut ops, 1)?;
        // remove_file(format!("{}_server_{}", temp_file, 0))?;
        assert_eq!(log_entries.len(), read_log_entries.len());
        Ok(())
    }

    #[test]
    fn test_log_entries_write_and_read_from_offset() -> Result<(), io::Error> {
        info!("Starting test_log_entries_write_and_read_from_offset");
        let _ = env_logger::try_init();
        let temp_file = "temp_file";
        let mut ops = DirectFileOpsWriter::new(temp_file, 0)?;
        <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::write_term_and_voted_for(
            &mut ops,
            1,
            Option::Some(2),
        )?;

        //  generate a bunch of logs and write them
        let mut log_entries: Vec<LogEntry<TestEntryData>> = Vec::new();
        let mut log_index: u64 = 1;
        for i in 0..100 {
            let log_entry: LogEntry<TestEntryData> = LogEntry {
                term: 1,
                index: log_index,
                command: LogEntryCommand::Set,
                key: i.to_string(),
                value: TestEntryData(format!("value_{}", i)),
            };
            log_entries.push(log_entry);
            log_index += 1;
        }
        ops.append_logs(&log_entries)?;

        //  reading your own writes from some offset here
        let read_log_entries =
            <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::read_logs(&mut ops, 50)?;
        remove_file(format!("{}_server_{}", temp_file, 0))?;
        assert_eq!(log_entries.len() - 49, read_log_entries.len());
        Ok(())
    }

    #[test]
    fn test_log_entries_write_at_offset_and_read() -> Result<(), io::Error> {
        info!("Starting test_log_entries_write_at_offset_and_read");
        let _ = env_logger::try_init();
        let temp_file = "temp_file";
        let mut ops = DirectFileOpsWriter::new(temp_file, 0)?;
        <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::write_term_and_voted_for(
            &mut ops,
            1,
            Option::Some(2),
        )?;

        //  generate a bunch of logs and write them
        let mut log_entries: Vec<LogEntry<TestEntryData>> = Vec::new();
        let mut log_index: u64 = 1;
        for i in 0..100 {
            let log_entry: LogEntry<TestEntryData> = LogEntry {
                term: 1,
                index: log_index,
                command: LogEntryCommand::Set,
                key: i.to_string(),
                value: TestEntryData(format!("value_{}", i)),
            };
            log_entries.push(log_entry);
            log_index += 1;
        }
        ops.append_logs(&log_entries)?;

        //  generate a new set of logs
        let mut new_log_entries: Vec<LogEntry<TestEntryData>> = Vec::new();
        let mut new_log_index: u64 = 20;
        for i in 0..20 {
            let log_entry: LogEntry<TestEntryData> = LogEntry {
                term: 1,
                index: new_log_index,
                command: LogEntryCommand::Set,
                key: i.to_string(),
                value: TestEntryData(format!("value_{}", i)),
            };
            new_log_entries.push(log_entry);
            new_log_index += 1;
        }
        ops.append_logs_at(&new_log_entries, 20)?;

        //  read the logs
        let read_log_entries =
            <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::read_logs(&mut ops, 1)?;
        remove_file(format!("{}_server_{}", temp_file, 0))?;
        assert_eq!(read_log_entries.len(), 40);
        Ok(())
    }

    #[test]
    fn test_overwrite_term_and_voted_for_with_logs_present() -> Result<(), io::Error> {
        info!("Starting test_log_entries_write_at_offset_and_read");
        let _ = env_logger::try_init();
        let temp_file = "temp_file";
        let mut ops = DirectFileOpsWriter::new(temp_file, 0)?;
        <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::write_term_and_voted_for(
            &mut ops, 1, None,
        )?;

        //  generate a bunch of logs and write them
        let mut log_entries: Vec<LogEntry<TestEntryData>> = Vec::new();
        let mut log_index: u64 = 1;
        for i in 0..100 {
            let log_entry: LogEntry<TestEntryData> = LogEntry {
                term: 1,
                index: log_index,
                command: LogEntryCommand::Set,
                key: i.to_string(),
                value: TestEntryData(format!("value_{}", i)),
            };
            log_entries.push(log_entry);
            log_index += 1;
        }
        ops.append_logs(&log_entries)?;

        //  read your own writes here
        let read_log_entries =
            <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::read_logs(&mut ops, 1)?;
        assert_eq!(log_entries.len(), read_log_entries.len());

        //  overwrite the term and voted for
        <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::write_term_and_voted_for(
            &mut ops,
            2,
            Some(1),
        )?;
        let (term, voted_for) =
            <DirectFileOpsWriter as RaftFileOps<TestEntryData>>::read_term_and_voted_for(&ops)?;
        assert_eq!(term, 2);
        assert_eq!(voted_for, 1);

        Ok(())
    }
}
