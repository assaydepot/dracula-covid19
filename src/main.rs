use reqwest::Error;

const CONFIRMED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv";
const _DEATHS_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv";
const _RECOVERED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv";



#[tokio::main]
async fn main() -> Result<(),DracErr> {

    let req = reqwest::get(CONFIRMED_URL).await?;
    let bytes = req.bytes().await?;
    let bytes_reader = std::io::Cursor::new(&bytes[..]);

    let mut reader = csv::ReaderBuilder::new().from_reader(bytes_reader);

    for row in reader.records() {
        println!("{:#?}", row);
    }

    // let reader = csv::ReaderBuilder::new().from_reader();

    Ok(())
}

#[derive(Debug)]
pub enum DracErr {
    Reqwest(reqwest::Error),
}

impl From<reqwest::Error> for DracErr {
    fn from(e: reqwest::Error) -> Self {
        DracErr::Reqwest(e)
    }
}