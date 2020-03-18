//use dracula_covid19::*;
use crate::aws::*;
use crate::cleaner::*;
use crate::error::DracErr;
use crate::parquet_writer::write_records_to_file_population;
use parquet::record::{RecordSchema, RecordWriter};
use std::env;

use serde::Deserialize;
use std::error::Error;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::process;

#[derive(Debug, Deserialize, ParquetRecordWriter, ParquetRecordSchema)]
pub struct Population {
    id: i32,
    #[serde(deserialize_with = "csv::invalid_option")]
    country: Option<String>,
    population: i32,
    yearly_change: f64,
    net_change: i32,
    density_p_sq_km: f32,
    land_area_sq_km: i32,

    migrants_net: Option<i32>,
    #[serde(deserialize_with = "csv::invalid_option")]
    fert_rate: Option<f32>,
    #[serde(deserialize_with = "csv::invalid_option")]
    med_age: Option<i32>,
    #[serde(deserialize_with = "csv::invalid_option")]
    urban_pop: Option<f32>,
    #[serde(deserialize_with = "csv::invalid_option")]
    world_share: Option<f32>,
}
const POPULATION_URL: &str = "world_pop2020.csv";

#[tokio::main]
pub async fn example() -> Result<(), Box<dyn Error>> {
    //async fn example() -> Result<(), Box<dyn Error>> {
    //    let file_path = get_first_arg()?;
    let mut recs: Vec<Population> = Vec::new();
    let file_path = OsString::from("world_pop2020.csv");
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    //    let mut rdr = csv::Reader::from_reader(io::stdin());
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let record: Population = result?;
        println!("{:?}", &record);
        recs.push(record);
    }
    write_records_to_file_population("population.parquet", recs);

    let bucket = "scientist-datawarehouse".to_string();
    let key = "Population/population.parquet".to_string();
    //    let crawler_name = "population".to_string();
    let crawler_name = "population".to_string();

    let key_parts = key.split('/').collect::<Vec<&str>>();
    let key_dir = key_parts[0..key_parts.len() - 1].join("/");
    let s3_path = format!("s3://{}/{}", bucket, key_dir);

//    use futures::future::ok;
    upload_file("population.parquet", bucket.clone(), key.clone())
        .await
        .unwrap();

    create_crawler(crawler_name.clone(), s3_path)
        .await
        .unwrap();
    start_crawler(crawler_name.clone(), true)
        .await
        .unwrap();

    Ok(())
}

pub fn world_pop() {

    if let Err(err) = example() {
        println!("{}", err);
        process::exit(1);
    }
}
