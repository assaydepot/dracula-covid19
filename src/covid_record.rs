use parquet::record::{RecordSchema, RecordWriter};

#[derive(ParquetRecordWriter, ParquetRecordSchema)]
pub struct CovidRecord {
    pub province_state: String,
    pub state: String,
    pub city: String,
    pub country_region: String,
    pub lat: String,
    pub long: String,
    pub date: chrono::NaiveDateTime,
    pub count: i64,
}
