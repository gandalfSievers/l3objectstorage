use bytes::Bytes;
use http_body_util::Full;
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

/// Sends notification messages to local SNS/SQS emulators via HTTP.
pub struct NotificationSender {
    pub sns_endpoint: Option<String>,
    pub sqs_endpoint: Option<String>,
}

impl NotificationSender {
    pub fn new(sns_endpoint: Option<String>, sqs_endpoint: Option<String>) -> Self {
        Self {
            sns_endpoint,
            sqs_endpoint,
        }
    }

    /// Publish a message to an SNS topic.
    ///
    /// POSTs to `{sns_endpoint}/` with form body `Action=Publish&TopicArn=...&Message=...`.
    pub async fn publish_to_sns(&self, topic_arn: &str, message: &str) -> Result<(), String> {
        let endpoint = self
            .sns_endpoint
            .as_ref()
            .ok_or_else(|| "SNS endpoint not configured".to_string())?;

        let body = format!(
            "Action=Publish&TopicArn={}&Message={}",
            utf8_percent_encode(topic_arn, NON_ALPHANUMERIC),
            utf8_percent_encode(message, NON_ALPHANUMERIC),
        );

        let uri = format!("{}/", endpoint.trim_end_matches('/'));

        let req = Request::post(&uri)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| format!("Failed to build SNS request: {}", e))?;

        let client = Client::builder(TokioExecutor::new()).build_http();
        let resp = client
            .request(req)
            .await
            .map_err(|e| format!("SNS publish failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("SNS publish returned status {}", resp.status()));
        }

        Ok(())
    }

    /// Send a message to an SQS queue.
    ///
    /// Extracts the queue name from the ARN (last `:` segment), then POSTs to
    /// `{sqs_endpoint}/queue/{queue_name}` with form body `Action=SendMessage&MessageBody=...`.
    pub async fn send_to_sqs(&self, queue_arn: &str, message: &str) -> Result<(), String> {
        let endpoint = self
            .sqs_endpoint
            .as_ref()
            .ok_or_else(|| "SQS endpoint not configured".to_string())?;

        let queue_name = queue_arn
            .rsplit(':')
            .next()
            .ok_or_else(|| format!("Invalid queue ARN: {}", queue_arn))?;

        let queue_url = format!(
            "{}/queue/{}",
            endpoint.trim_end_matches('/'),
            queue_name
        );

        let body = format!(
            "Action=SendMessage&MessageBody={}",
            utf8_percent_encode(message, NON_ALPHANUMERIC),
        );

        let req = Request::post(&queue_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| format!("Failed to build SQS request: {}", e))?;

        let client = Client::builder(TokioExecutor::new()).build_http();
        let resp = client
            .request(req)
            .await
            .map_err(|e| format!("SQS send failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("SQS send returned status {}", resp.status()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_name_extracted_from_arn() {
        let arn = "arn:aws:sqs:us-east-1:000000000000:my-queue";
        let queue_name = arn.rsplit(':').next().unwrap();
        assert_eq!(queue_name, "my-queue");
    }

    #[test]
    fn sns_no_endpoint_returns_error() {
        let sender = NotificationSender::new(None, None);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(sender.publish_to_sns("arn:aws:sns:us-east-1:000:topic", "msg"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not configured"));
    }

    #[test]
    fn sqs_no_endpoint_returns_error() {
        let sender = NotificationSender::new(None, None);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(sender.send_to_sqs("arn:aws:sqs:us-east-1:000:queue", "msg"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not configured"));
    }
}
