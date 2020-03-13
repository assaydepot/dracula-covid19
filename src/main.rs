use dracula_covid19::*;

const CONFIRMED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv";
const DEATHS_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv";
const RECOVERED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv";

#[tokio::main]
async fn main() -> Result<(), DracErr> {
    let mut records: Vec<CovidRecord> = Vec::new();

    extract_records(CONFIRMED_URL, &mut records).await.unwrap();
    extract_records(DEATHS_URL, &mut records).await.unwrap();
    extract_records(RECOVERED_URL, &mut records).await.unwrap();

    write_records_to_file("combined.parquet", records);

    let bucket = "scientist-datawarehouse".to_string();
    let key =
        "csse_covid_19_time_series/combined/time_series_19-covid-Combined.parquet".to_string();
    let crawler_name = "covid19-combined".to_string();
    upload_file("combined.parquet", bucket.clone(), key.clone())
        .await
        .unwrap();
    create_crawler(crawler_name.clone(), format!("{}/{}", bucket, key))
        .await
        .unwrap();
    start_crawler(crawler_name.clone(), true).await.unwrap();

    Ok(())
}

async fn extract_records(input_url: &str, records: &mut Vec<CovidRecord>) -> Result<(), DracErr> {
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
                if res.is_err() {
                    panic!("could not parse `{}`", date_str);
                } else {
                    res.unwrap()
                }
            })
            .collect()
    };

    for row in reader.records() {
        let row = row?;
        let mut row_iter = row.iter();

        let province_state = row_iter.next().unwrap().to_string();
        let state = "dummy state".to_string();

        // California, US
        // Los Angeles, CA
        // let state = if province_state.find(',').is_some() {
        //     province_state
        //         .split_at(province_state.len() - 2)
        //         .to_string()
        // } else {
        //     "".to_string()
        // };

        let city = "dummy city".to_string();

        // let city = if province_state.is_some() && province_state.find(',').is_some() {
        //     &&province_state[0..province_state.find(',').unwrap_or(0)].to_string()
        // } else {
        //     "".to_string()
        // };

        let country_region = row_iter.next().unwrap().to_string();
        let lat = row_iter.next().unwrap().to_string();
        let long = row_iter.next().unwrap().to_string();

        for date in dates.iter() {
            let date_count_str = row_iter.next().unwrap();
            let count: i64 = date_count_str.parse().unwrap();

            records.push(CovidRecord {
                province_state: province_state.clone(),
                state: state.clone(),
                city: city.clone(),
                country_region: country_region.clone(),
                lat: lat.clone(),
                long: long.clone(),
                date: date.clone(),
                count,
            })
        }
    }

    Ok(())
}

// async fn convert_and_upload(
//     input_url: &str,
//     parquet_name: &str,
//     bucket: String,
//     key: String,
// ) -> Result<(), DracErr> {
//     let path = parquet_name;
//     let req = reqwest::get(input_url).await?;
//     let bytes = req.bytes().await?;
//     let bytes_reader = std::io::Cursor::new(&bytes[..]);
//
//     let mut reader = csv::ReaderBuilder::new().from_reader(bytes_reader);
//
//     let mut parquet_writer = parquet_writer::<CovidRecord>(path).unwrap();
//
//     let dates: Vec<chrono::NaiveDateTime> = {
//         let headers = reader.headers()?;
//         let mut header_iter = headers.iter();
//         header_iter.next(); // Province
//         header_iter.next(); // Country
//         header_iter.next(); // Lat
//         header_iter.next(); // Long
//         header_iter
//             .map(|date_str| {
//                 // panic!("{}", date_str);
//                 let res = chrono::NaiveDateTime::parse_from_str(
//                     &format!("{} 00:00", date_str),
//                     "%-m/%-d/%y %H:%M",
//                 );
//                 if res.is_err() {
//                     panic!("could not parse `{}`", date_str);
//                 } else {
//                     res.unwrap()
//                 }
//             })
//             .collect()
//     };
//
//     let mut records: Vec<CovidRecord> = Vec::new();
//
//     for row in reader.records() {
//         let row = row?;
//         let mut row_iter = row.iter();
//
//         let province_state = row_iter.next().unwrap().to_string();
//         let state = if province_state.is_some() && province_state.find(',').is_some() {
//             province_state
//                 .split_at(province_state.len() - 2)
//                 .to_string()
//         } else {
//             "".to_string()
//         };
//         let city = if province_state.is_some() && province_state.find(',').is_some() {
//             &&province_state[0..province_state.find(',').unwrap_or(0)].to_string()
//         } else {
//             "".to_string()
//         };
//
//         let country_region = row_iter.next().unwrap().to_string();
//         let lat = row_iter.next().unwrap().to_string();
//         let long = row_iter.next().unwrap().to_string();
//
//         for date in dates.iter() {
//             let date_count_str = row_iter.next().unwrap();
//             let count: i64 = date_count_str.parse().unwrap();
//
//             records.push(CovidRecord {
//                 province_state: province_state.clone(),
//                 state: state.clone(),
//                 city: city.clone(),
//                 country_region: country_region.clone(),
//                 lat: lat.clone(),
//                 long: long.clone(),
//                 date: date.clone(),
//                 count,
//             })
//         }
//     }
//
//     let mut row_group = parquet_writer.next_row_group().unwrap();
//     (&records[..]).write_to_row_group(&mut row_group).unwrap();
//     parquet_writer.close_row_group(row_group).unwrap();
//
//     parquet_writer.close().unwrap();
//
//     upload_file(path, bucket.clone(), key.clone())
//         .await
//         .unwrap();
//
//     let key_parts: Vec<&str> = key.split("/").collect();
//     let glue_dir_path = (&key_parts[0..key_parts.len() - 1]).join("/");
//     let s3_path = format!("{}/{}/", bucket, glue_dir_path);
//     println!("{}", s3_path);
//
//     let crawler_name = format!("covid19-{}-crawler", parquet_name.replace(".parquet", ""));
//     create_crawler(crawler_name.clone(), s3_path).await.unwrap();
//     start_crawler(crawler_name, true).await.unwrap();
//
//     // crawl()
//
//     Ok(())
// }

// California,US
// "Los Angeles, CA",US
// "Shasta County, CA",US
