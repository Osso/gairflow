use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

pub(super) struct MockAirflowServer {
    pub(super) base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MockAirflowServer {
    pub(super) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);

        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                handle_request(stream, address, &thread_requests);
            }
        });

        Self {
            base_url: format!("http://{}", display_addr(address)),
            requests,
        }
    }

    pub(super) fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

fn handle_request(
    mut stream: std::net::TcpStream,
    address: SocketAddr,
    requests: &Arc<Mutex<Vec<String>>>,
) {
    let mut buffer = [0_u8; 8192];
    let bytes = stream.read(&mut buffer).unwrap();
    let request = String::from_utf8_lossy(&buffer[..bytes]).to_string();
    let request_line = request.lines().next().unwrap_or_default().to_string();
    requests
        .lock()
        .unwrap()
        .push(format!("{request_line}\n{request}"));

    let (status, body) = response_for(&request_line, address);
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).unwrap();
}

fn response_for(request_line: &str, address: SocketAddr) -> (&'static str, String) {
    if request_line.contains("/status-error") {
        (
            "500 Internal Server Error",
            serde_json::json!({ "error": "failed" }).to_string(),
        )
    } else {
        ("200 OK", response_body(request_line, address))
    }
}

fn response_body(request_line: &str, address: SocketAddr) -> String {
    if request_line.starts_with("GET /projects/") {
        composer_environment_json(address)
    } else if request_line.contains("/invalid-json") {
        "not-json".to_string()
    } else if request_line.starts_with("POST /token") {
        serde_json::json!({ "access_token": "refreshed-token" }).to_string()
    } else if request_line.starts_with("GET /airflow/api/v2/dags?") {
        schedules_json()
    } else if request_line.contains("/dagRuns?") {
        serde_json::json!({ "dag_runs": [run_json()] }).to_string()
    } else if request_line.contains("/dagRuns/run-1/taskInstances") {
        serde_json::json!({ "task_instances": [] }).to_string()
    } else if request_line.contains("/dagRuns/run-1") {
        run_json().to_string()
    } else if request_line.contains("/tasks/task-1") {
        serde_json::json!({ "task_id": "task-1" }).to_string()
    } else if request_line.contains("/tasks") {
        tasks_json()
    } else if request_line.starts_with("GET /airflow/api/v2/dags/sync") {
        schedule_json()
    } else {
        serde_json::json!({ "ok": true }).to_string()
    }
}

fn composer_environment_json(address: SocketAddr) -> String {
    serde_json::json!({
        "config": {
            "airflowUri": format!("http://{}/airflow", display_addr(address))
        }
    })
    .to_string()
}

fn schedules_json() -> String {
    serde_json::json!({
        "dags": [{
            "dag_id": "sync",
            "is_paused": false,
            "next_dagrun_run_after": "2026-06-23T01:00:00Z",
            "timetable_summary": "@hourly"
        }]
    })
    .to_string()
}

fn tasks_json() -> String {
    serde_json::json!({
        "tasks": [{
            "task_id": "task-1",
            "operator_name": "PythonOperator",
            "retries": 3,
            "pool": "default_pool"
        }]
    })
    .to_string()
}

fn schedule_json() -> String {
    serde_json::json!({
        "dag_id": "sync",
        "next_dagrun": "run-2",
        "next_dagrun_data_interval_start": "2026-06-23T01:00:00Z",
        "next_dagrun_data_interval_end": "2026-06-23T02:00:00Z",
        "is_paused": false
    })
    .to_string()
}

pub(super) fn run_json() -> serde_json::Value {
    serde_json::json!({
        "dag_id": "sync",
        "dag_run_id": "run-1",
        "state": "success",
        "run_type": "scheduled",
        "run_after": "2026-06-23T00:00:00Z",
        "start_date": "2026-06-23T00:01:00Z",
        "end_date": "2026-06-23T00:03:00Z",
        "duration": 120,
        "conf": { "table": "pages" }
    })
}

fn display_addr(addr: SocketAddr) -> String {
    format!("{}:{}", addr.ip(), addr.port())
}

pub(super) fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{prefix}-{nanos}"));
    std::fs::create_dir_all(&path).unwrap();
    path
}

pub(super) fn write_fake_gcloud(bin_dir: &std::path::Path) {
    let path = bin_dir.join("gcloud");
    std::fs::write(
            &path,
            r#"#!/bin/sh
if [ "$1" = "storage" ] && [ "$2" = "cat" ]; then
  printf '%s\n' '{"include_tables":["pages"],"expected_skip_tables":["sessions","paid_users_pages","paid_subscriptions_users_pages"],"bootstrap_acknowledged":[]}'
  exit 0
fi
exit 0
"#,
        )
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}
