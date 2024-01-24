use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080")?;

    // Construct a simulated RequestVoteRequest JSON
    let request_vote = json!({
        "term": 1,
        "candidate_id": "12345678-1234-1234-1234-1234567890ab", // Use a valid UUID
        "last_log_index": 0,
        "last_log_term": 0,
    });
    let serialized_request = serde_json::to_string(&request_vote).unwrap() + "\n";

    // Send the request
    stream.write_all(serialized_request.as_bytes())?;

    // Read the response
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    println!("Response from server: {}", response);

    Ok(())
}
