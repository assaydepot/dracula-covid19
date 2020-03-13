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
