use parquet::errors::ParquetError;
use parquet::file::writer::FileWriter;
use parquet::record::{RecordSchema, RecordWriter};

use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;

use std::fs::File;
use std::rc::Rc;

use crate::CovidRecord;

pub fn write_records_to_file(path: &str, records: Vec<CovidRecord>) {
    let mut parquet_writer = parquet_writer::<CovidRecord>(path).unwrap();

    let mut row_group = parquet_writer.next_row_group().unwrap();
    (&records[..]).write_to_row_group(&mut row_group).unwrap();
    parquet_writer.close_row_group(row_group).unwrap();

    parquet_writer.close().unwrap();
}

pub fn parquet_writer<R: RecordSchema>(
    path: &str,
) -> Result<SerializedFileWriter<File>, ParquetError> {
    let props = WriterProperties::builder().set_compression(parquet::basic::Compression::GZIP);
    let props = Rc::new(props.build());
    let schema = R::schema();
    let file = File::create(path).unwrap();

    SerializedFileWriter::new(file, Rc::new(schema), props)
}
