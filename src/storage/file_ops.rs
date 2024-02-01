use std::{
    fmt::Display,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Seek, SeekFrom, Write},
    str::FromStr,
};

use crate::{
    types::{ServerId, Term},
    LogEntry, LogEntryCommand,
};

trait BaseFileOps {
    fn read_term_and_voted_for(&self) -> Result<(Term, ServerId), io::Error>;
    fn write_term_and_voted_for(
        &mut self,
        term: Term,
        voted_for: ServerId,
    ) -> Result<(), io::Error>;
}

trait GenericFileOps<T: Clone + FromStr + Display>
where
    T::Err: Display,
{
    fn read_logs(&mut self, log_index: u64) -> Result<Vec<LogEntry<T>>, io::Error>;
    fn append_logs(&mut self, entries: &Vec<LogEntry<T>>) -> Result<(), io::Error>;
}

pub struct DirectFileOps {
    file_path: String,
    file: Option<File>,
}

impl DirectFileOps {
    fn new(file_path: &str, server_id: ServerId) -> Result<Self, io::Error> {
        let file_name = format!("{}_server_{}", file_path, server_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_name)?;

        Ok(DirectFileOps {
            file_path: file_name,
            file: Some(file),
        })
    }
}

impl BaseFileOps for DirectFileOps {
    fn read_term_and_voted_for(&self) -> Result<(Term, ServerId), io::Error> {
        let mut file = self.file.as_ref().expect("File not found");
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

    fn write_term_and_voted_for(
        &mut self,
        term: Term,
        voted_for: ServerId,
    ) -> Result<(), io::Error> {
        let file = self.file.as_mut().expect("File not found");
        file.seek(SeekFrom::Start(0))?;
        writeln!(file, "{},{}", term, voted_for)?;
        file.flush()?;
        Ok(())
    }
}

impl<T: Clone + FromStr + Display> GenericFileOps<T> for DirectFileOps
where
    T::Err: Display,
{
    fn read_logs(&mut self, log_index: u64) -> Result<Vec<LogEntry<T>>, io::Error> {
        let file = self.file.as_mut().expect("File not found");
        let mut buf_reader = BufReader::new(file);
        let mut line = String::new();
        buf_reader.read_line(&mut line)?;
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
            let line = line?;
            let values: Vec<&str> = line.split(',').collect();
            let term: Term = values[0].trim().parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid term: {}", e))
            })?;
            let index: u64 = values[1]
                .trim()
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let command: LogEntryCommand = values[2]
                .trim()
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let key: String = values[3].trim().to_string();
            let value: T = values[4].trim().parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid value: {}", e))
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
        let file = self.file.as_mut().expect("File not found");
        file.seek(SeekFrom::End(0))?;
        for entry in entries {
            writeln!(
                file,
                "{},{},{},{},{}",
                entry.term, entry.index, entry.command, entry.key, entry.value
            )?;
        }
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
    use std::io;

    use log::debug;

    use super::*;
    use super::{BaseFileOps, DirectFileOps, GenericFileOps};

    #[test]
    fn test_term_and_voted_for_read_and_write() -> Result<(), io::Error> {
        let temp_file = "temp_file";
        let mut ops = DirectFileOps::new(temp_file, 0)?;
        ops.write_term_and_voted_for(1, 2)?;
        let (term, voted_for) = ops.read_term_and_voted_for()?;
        // debug!("{},{}", term, voted_for);
        // assert_eq!(term, 1);
        // assert_eq!(voted_for, 2);
        Ok(())
    }

    #[test]
    fn test_log_entries_read_and_write() -> Result<(), io::Error> {
        Ok(())
    }
}
