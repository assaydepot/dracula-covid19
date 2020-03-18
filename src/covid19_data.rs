//use dracula_covid19::*;
use crate::error::DracErr;
//use sentry::integrations::panic::register_panic_handler;

use crate::CovidRecord;
use crate::aws::*;
use crate::cleaner::*;
use crate::parquet_writer::write_records_to_file;

const CONFIRMED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv";
const DEATHS_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv";
const RECOVERED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv";

#[tokio::main]
pub async fn covid19_data() -> Result<(), DracErr> {
    let mut records: Vec<CovidRecord> = Vec::new();

    extract_records(CONFIRMED_URL, "confirmed", &mut records)
        .await
        .unwrap();
    extract_records(DEATHS_URL, "deaths", &mut records)
        .await
        .unwrap();
    extract_records(RECOVERED_URL, "recovered", &mut records)
        .await
        .unwrap();

    write_records_to_file("combined.parquet", records);

    let bucket = "scientist-datawarehouse".to_string();
    let key =
        "csse_covid_19_time_series/combined/time_series_19-covid-Combined.parquet".to_string();
    let crawler_name = "covid19-combined".to_string();

    let key_parts = key.split('/').collect::<Vec<&str>>();
    let key_dir = key_parts[0..key_parts.len() - 1].join("/");
    let s3_path = format!("s3://{}/{}", bucket, key_dir);

    upload_file("combined.parquet", bucket.clone(), key.clone())
        .await
        .unwrap();
    create_crawler(crawler_name.clone(), s3_path).await.unwrap();
    start_crawler(crawler_name.clone(), true).await.unwrap();

    Ok(())
}

async fn extract_records(
    input_url: &str,
    status: &str,
    records: &mut Vec<CovidRecord>,
) -> Result<(), DracErr> {
    let req = reqwest::get(input_url).await?;
    let bytes = req.bytes().await?;
    let bytes_reader = std::io::Cursor::new(&bytes[..]);

    let mut reader = csv::ReaderBuilder::new().from_reader(bytes_reader);

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
                if let Ok(res) = res {
                    res
                } else {
                    panic!("could not parse `{}`", date_str)
                }
            })
            .collect()
    };

    for row in reader.records() {
        let row = row?;
        let mut row_iter = row.iter();

        let province_state = row_iter.next().unwrap().to_string();
        let province_state = if province_state == "" {
            None
        } else {
            Some(province_state)
        };

        let country_region = row_iter.next().unwrap().to_string();

        let (city, county, state) = if country_region == "US" {
            extract_us_data(&province_state.as_ref().unwrap()[..])
        } else {
            (None, None, None)
        };

        let lat = row_iter.next().unwrap().to_string();
        let long = row_iter.next().unwrap().to_string();

        for date in dates.iter().cloned() {
            let date_count_str = row_iter.next().unwrap();

            let count: i64 = date_count_str.parse().unwrap_or_default();

            let lat = if lat == "" {
                None
            } else {
                Some(lat.parse().unwrap())
            };

            let lon = if long == "" {
                None
            } else {
                Some(long.parse().unwrap())
            };

            let mut record = CovidRecord {
                status: status.to_string(),
                province_state: province_state.clone(),
                state: state.clone(),
                county: county.clone(),
                city: city.clone(),
                country_region: country_region.clone(),
                lat,
                lon,
                date,
                count,
            };

            remap_territories(&mut record);

            records.push(record)
        }
    }

    Ok(())
}
