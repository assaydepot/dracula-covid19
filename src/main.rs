#[macro_use]
extern crate parquet_derive;
use parquet::errors::ParquetError;
use parquet::file::writer::FileWriter;
use parquet::record::{RecordSchema, RecordWriter};

use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use rusoto_core::Region;
use rusoto_glue::Glue;
use rusoto_s3::{PutObjectRequest, StreamingBody, S3};
use std::fs::File;
use std::path::Path;
use std::rc::Rc;

const CONFIRMED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv";
const DEATHS_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv";
const RECOVERED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv";

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
    convert_and_upload(
        CONFIRMED_URL,
        "confirmed.parquet",
        "scientist-datawarehouse".into(),
        "csse_covid_19_time_series/confirmed/time_series_19-covid-Confirmed.parquet"
            .to_string(),
    )
    .await
    .unwrap();
    convert_and_upload(
        DEATHS_URL,
        "deaths.parquet",
        "scientist-datawarehouse".into(),
        "csse_covid_19_time_series/deaths/time_series_19-covid-Deaths.parquet".to_string(),
    )
    .await
    .unwrap();
    convert_and_upload(
        RECOVERED_URL,
        "recovered.parquet",
        "scientist-datawarehouse".into(),
        "csse_covid_19_time_series/recovered/time_series_19-covid-Recovered.parquet"
            .to_string(),
    )
    .await
    .unwrap();

    Ok(())
}

async fn convert_and_upload(
    input_url: &str,
    parquet_name: &str,
    bucket: String,
    key: String,
) -> Result<(), DracErr> {
    let path = parquet_name;
    let req = reqwest::get(input_url).await?;
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

    upload_file(path, bucket.clone(), key.clone())
        .await
        .unwrap();

    let key_parts: Vec<&str> = key.split("/").collect();
    let glue_dir_path = (&key_parts[0..key_parts.len() - 1]).join("/");
    let s3_path = format!("{}/{}/", bucket, glue_dir_path);
    println!("{}", s3_path);

    let crawler_name = format!("covid19-{}-crawler", parquet_name.replace(".parquet", ""));
    create_crawler(crawler_name.clone(), s3_path).await.unwrap();
    start_crawler(crawler_name, true).await.unwrap();

    // crawl()

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

pub async fn create_crawler(crawler_name: String, s3_path: String) -> Result<(), ()> {
    if s3_path.split('/').last().unwrap().split('.').last() == Some(".parquet") {
        println!("s3_path be a bucket subpath, not a parquet file");
        return Err(());
    }

    let request = rusoto_glue::CreateCrawlerRequest {
        classifiers: None,
        configuration: None,
        database_name: Some("datascience_parquet".to_string()),
        description: None,
        name: crawler_name.clone(),
        role: "arn:aws:iam::554546661178:role/service-role/AWSGlueServiceRole-datascience"
            .to_string(),
        schedule: None,
        schema_change_policy: None,
        table_prefix: None,
        targets: rusoto_glue::CrawlerTargets {
            dynamo_db_targets: None,
            jdbc_targets: None,
            s3_targets: Some(vec![rusoto_glue::S3Target {
                exclusions: None,
                path: Some(s3_path),
            }]),
            catalog_targets: None,
        },
        tags: None,
        crawler_security_configuration: None,
    };

    let glue = rusoto_glue::GlueClient::new(Region::default());

    let result = glue
        .get_crawler(rusoto_glue::GetCrawlerRequest { name: crawler_name })
        .await;
    let must_create = match result {
        Ok(_) => false,
        Err(rusoto_core::RusotoError::Service(rusoto_glue::GetCrawlerError::EntityNotFound(_))) => {
            true
        }
        f => panic!("unhandled crawler error: {:#?}", f),
    };
    if must_create {
        let result = glue.create_crawler(request).await.expect("create crawler");
        println!("result: {:?}", result);
    } else {
        println!("crawler already exists")
    }
    Ok(())
}

pub fn delay_time() -> std::time::Duration {
    std::time::Duration::from_secs(10)
}

pub async fn start_crawler(crawler_name: String, poll_to_completion: bool) -> Result<(), ()> {
    let glue = rusoto_glue::GlueClient::new(Region::default());

    let mut attempts = 0;
    loop {
        let result = glue
            .start_crawler(rusoto_glue::StartCrawlerRequest {
                name: crawler_name.clone(),
            })
            .await;
        attempts += 1;

        match result {
            Ok(_) => {
                println!("crawling away on {}", crawler_name);
                break;
            }
            Err(crawler_error) => match crawler_error {
                rusoto_core::RusotoError::Service(
                    rusoto_glue::StartCrawlerError::CrawlerRunning(_),
                ) => {
                    if !poll_to_completion {
                        println!("crawler failed. bailing out.");
                        break;
                    } else {
                        if attempts < 20 {
                            println!("crawler already running, retrying in 5 seconds")
                        } else {
                            panic!("crawler has tried 20 times. dying")
                        }
                        std::thread::sleep(delay_time());
                    }
                }
                f => unimplemented!("don't know {:#?}", f),
            },
        };
    }

    if poll_to_completion {
        wait_for_crawler(&glue, crawler_name).await?
    }

    Ok(())
}

async fn wait_for_crawler(glue: &rusoto_glue::GlueClient, crawler_name: String) -> Result<(), ()> {
    loop {
        let response = glue
            .get_crawler(rusoto_glue::GetCrawlerRequest {
                name: crawler_name.clone(),
            })
            .await; // .map_err(|_| ())?

        match response {
            Ok(crawler_resp) => {
                if let Some(crawler) = crawler_resp.crawler {
                    if crawler.state == Some("RUNNING".into()) {
                        println!("crawler is RUNNING, going to sleep {:?}", delay_time());
                        std::thread::sleep(delay_time());
                        continue;
                    } else if crawler.state == Some("STOPPING".into()) {
                        println!(
                            "crawler is stopping... will check for READY in {:?}",
                            delay_time()
                        );
                        std::thread::sleep(delay_time());
                        continue;
                    } else if crawler.state == Some("READY".into()) {
                        println!("crawler is done!");
                        break;
                    } else {
                        panic!("weird state, got {:?}", crawler.state)
                    }
                } else {
                    panic!("no crawler?!")
                }
            }
            Err(e) => panic!("error?! {:#?}", e),
        }
    }

    Ok(())
}
