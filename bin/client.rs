use std::thread;
use std::time::Duration;
use tokio;

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let api_ports = vec![3000];
    let mut counter = 0;

    loop {
        let api_port = api_ports[counter % api_ports.len()];
        counter += 1;

        // Construct the key-value pair
        let key = format!("{}", counter);
        let value = counter;

        // Construct the URL
        let url = format!("http://127.0.0.1:{}/submit/{}/{}", api_port, key, value);

        // Make the POST request
        let client = reqwest::Client::new();
        let response = client.post(&url).send().await?;

        // Read the response
        if response.status().is_success() {
            let response_body = response.text().await?;
            println!("Response from server {}: {}", api_port, response_body);
        } else {
            eprintln!(
                "Error response from server {}: {}",
                api_port,
                response.status()
            );
        }

        // Wait before sending the next request
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
