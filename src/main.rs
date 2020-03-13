#[macro_use]
extern crate parquet_derive;
use parquet::errors::ParquetError;
use parquet::file::writer::FileWriter;
use parquet::record::{RecordSchema, RecordWriter};

use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, StreamingBody, S3};
use std::fs::File;
use std::path::Path;
use std::rc::Rc;

const CONFIRMED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv";
const _DEATHS_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv";
const _RECOVERED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv";

#[derive(ParquetRecordWriter, ParquetRecordSchema)]
struct CovidRecord {
    province_state: String,
    country_region: String,
    lat: String,
    long: String,
    date: chrono::NaiveDateTime,
    count: i64,
}

#[tokio::main]
async fn main() -> Result<(), DracErr> {
    let path = "blah.parquet";
    let req = reqwest::get(CONFIRMED_URL).await?;
    let bytes = req.bytes().await?;
    let bytes_reader = std::io::Cursor::new(&bytes[..]);

    let mut reader = csv::ReaderBuilder::new().from_reader(bytes_reader);

    let mut parquet_writer = parquet_writer::<CovidRecord>(path).unwrap();

    let dates: Vec<chrono::NaiveDateTime> = {
        let headers = reader.headers()?;
        let mut header_iter = headers.iter();
        header_iter.next(); // Province
        header_iter.next(); // Country
        header_iter.next(); // Lat
        header_iter.next(); // Long
        header_iter
            .map(|date_str| {
                // panic!("{}", date_str);
                let res = chrono::NaiveDateTime::parse_from_str(
                    &format!("{} 00:00", date_str),
                    "%-m/%-d/%y %H:%M",
                );
                if res.is_err() {
                    panic!("could not parse `{}`", date_str);
                } else {
                    res.unwrap()
                }
            })
            .collect()
    };

    let mut records: Vec<CovidRecord> = Vec::new();

    for row in reader.records() {
        let row = row?;
        let mut row_iter = row.iter();

        let province_state = row_iter.next().unwrap().to_string();
        let country_region = row_iter.next().unwrap().to_string();
        let lat = row_iter.next().unwrap().to_string();
        let long = row_iter.next().unwrap().to_string();

        for date in dates.iter() {
            let date_count_str = row_iter.next().unwrap();
            let count: i64 = date_count_str.parse().unwrap();

            records.push(CovidRecord {
                province_state: province_state.clone(),
                country_region: country_region.clone(),
                lat: lat.clone(),
                long: long.clone(),
                date: date.clone(),
                count,
            })
        }
    }

    let mut row_group = parquet_writer.next_row_group().unwrap();
    (&records[..]).write_to_row_group(&mut row_group).unwrap();
    parquet_writer.close_row_group(row_group).unwrap();

    parquet_writer.close().unwrap();

    upload_file(
        path,
        "scientist-datawarehouse".to_string(),
        "who_covid_19_sit_rep_time_series/time_series_19-covid-Confirmed.parquet".to_string(),
    )
    .await
    .unwrap();

    Ok(())
}

#[derive(Debug)]
pub enum DracErr {
    Reqwest(reqwest::Error),
    Csv(csv::Error),
}

impl From<reqwest::Error> for DracErr {
    fn from(e: reqwest::Error) -> Self {
        DracErr::Reqwest(e)
    }
}

impl From<csv::Error> for DracErr {
    fn from(e: csv::Error) -> Self {
        DracErr::Csv(e)
    }
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

pub async fn upload_file<P: AsRef<Path>>(
    path: P,
    bucket: String,
    key: String,
) -> Result<(), DracErr> {
    let s3_client = rusoto_s3::S3Client::new(Region::default());

    let data = std::fs::read(path).unwrap();
    let data_len = data.len();
    let stream = StreamingBody::from(data);

    let res = s3_client
        .put_object(PutObjectRequest {
            body: Some(stream),
            key,
            bucket,
            content_length: Some(data_len as i64),
            ..Default::default()
        })
        .await;

    println!("{:#?}", res);

    Ok(())
}
