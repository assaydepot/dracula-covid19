use dracula_covid19::*;
use sentry::integrations::panic::register_panic_handler;
//use dracula_covid19::covid19_data::covid19_data;

const CONFIRMED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv";
const DEATHS_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv";
const RECOVERED_URL: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv";

//#[tokio::main]
//async
fn main() -> Result<(), ()> {
    let mut records: Vec<CovidRecord> = Vec::new();

    let _guard = sentry::init((
        "https://cbf0fb060dec424697851fcf59102991@sentry.scientist.com/12",
        sentry::ClientOptions {
            environment: Some("production".into()),
            ..Default::default()
        },
    ));

    register_panic_handler();

    covid19_data();

    world_pop();
    Ok(())
}
