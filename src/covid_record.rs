use parquet::record::{RecordSchema, RecordWriter};

#[derive(ParquetRecordWriter, ParquetRecordSchema)]
pub struct CovidRecord {
    pub status: String,
    pub province_state: Option<String>,
    pub state: Option<String>,
    pub city: Option<String>,
    pub county: Option<String>,
    pub country_region: String,
    pub lat: String,
    pub long: String,
    pub date: chrono::NaiveDateTime,
    pub count: i64,
}
