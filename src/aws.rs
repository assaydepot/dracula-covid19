use rusoto_core::Region;
use rusoto_glue::Glue;
use rusoto_s3::{PutObjectRequest, StreamingBody, S3};

use std::path::Path;

use crate::DracErr;

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

pub fn delay_time() -> std::time::Duration {
    std::time::Duration::from_secs(10)
}

pub async fn upload_file<P: AsRef<Path>>(
    path: P,
    bucket: String,
    key: String,
) -> Result<(), DracErr> {
    let s3_client = rusoto_s3::S3Client::new(rusoto_core::Region::default());

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
        database_name: Some("covid19".to_string()),
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
