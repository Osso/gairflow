use std::ffi::OsString;
use std::fs;
use std::io::ErrorKind;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use reqwest::blocking::Client;
use serde::Deserialize;

const DEFAULT_ENVIRONMENT: &str = "gc-composer";
const DEFAULT_LOCATION: &str = "us-central1";
const DEFAULT_PROJECT: &str = "globalcomix";
const DEFAULT_CONFIG_URI: &str =
    "gs://us-central1-gc-composer-02600f88-bucket/dags/sync_gc_db/sync_config.json";
const COMPOSER_API: &str = "https://composer.googleapis.com/v1";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[derive(Parser)]
#[command(name = "gairflow")]
#[command(about = "GlobalComix Airflow schedule, run, and task helper")]
struct Cli {
    #[arg(long, global = true, default_value = DEFAULT_ENVIRONMENT)]
    environment: String,

    #[arg(long, global = true, default_value = DEFAULT_LOCATION)]
    location: String,

    #[arg(long, global = true, default_value = DEFAULT_PROJECT)]
    project: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List schedules.
    Schedules(SchedulesArgs),
    /// Show next scheduled run summary, or full schedule JSON with --full.
    NextRun(NextRunArgs),
    /// List runs across all schedules, or filter with --schedule.
    Runs(RunsArgs),
    /// Show one run, or that run's task runs.
    Run(RunArgs),
    /// List task definitions for one schedule.
    Tasks(TasksArgs),
    /// Show full details for one task definition.
    Task(TaskArgs),
    /// Pause the schedule.
    Pause(ScheduleArgs),
    /// Unpause the schedule.
    Unpause(ScheduleArgs),
    /// Trigger a run, optionally with {"table": "..."} conf.
    Trigger(TriggerArgs),
    /// Read Cloud Logging entries for a schedule, run id, or table filter.
    Logs(LogsArgs),
    /// Check live sync_config.json for benchmark-table safety.
    ConfigCheck(ConfigCheckArgs),
    /// Pass raw Airflow CLI args through Composer.
    Raw(RawArgs),
}

#[derive(Args)]
struct RunsArgs {
    #[arg(long)]
    schedule: Option<String>,

    #[arg(long)]
    state: Option<String>,

    #[arg(long)]
    full: bool,
}

#[derive(Args)]
struct RunArgs {
    run_id: String,

    #[arg(long)]
    schedule: String,

    #[arg(long)]
    tasks: bool,

    #[arg(long)]
    full: bool,
}

#[derive(Args)]
struct SchedulesArgs {
    #[arg(long)]
    full: bool,
}

#[derive(Args)]
struct ScheduleArgs {
    #[arg(long)]
    schedule: String,
}

#[derive(Args)]
struct TaskArgs {
    task_id: String,

    #[arg(long)]
    schedule: String,
}

#[derive(Args)]
struct NextRunArgs {
    #[arg(long)]
    schedule: String,

    #[arg(long)]
    full: bool,
}

#[derive(Args)]
struct TriggerArgs {
    #[arg(long)]
    schedule: String,

    #[arg(long)]
    table: Option<String>,
}

#[derive(Args)]
struct LogsArgs {
    #[arg(long)]
    schedule: Option<String>,

    #[arg(long)]
    run_id: Option<String>,

    #[arg(long)]
    table: Option<String>,

    #[arg(long, default_value = "24h")]
    since: String,

    #[arg(long, default_value_t = 100)]
    limit: u32,
}

#[derive(Args)]
struct ConfigCheckArgs {
    #[arg(long, default_value = DEFAULT_CONFIG_URI)]
    uri: String,
}

#[derive(Args)]
struct RawArgs {
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<OsString>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Schedules(args) => schedules(&cli, args),
        Commands::NextRun(args) => next_run(&cli, args),
        Commands::Runs(args) => list_runs(&cli, args),
        Commands::Run(args) => run(&cli, args),
        Commands::Tasks(args) => tasks(&cli, args),
        Commands::Task(args) => task(&cli, &args.task_id),
        Commands::Pause(args) => patch_schedule(&cli, args, true),
        Commands::Unpause(args) => patch_schedule(&cli, args, false),
        Commands::Trigger(args) => trigger(&cli, args),
        Commands::Logs(args) => logs(&cli, args),
        Commands::ConfigCheck(args) => config_check(&args.uri),
        Commands::Raw(args) => raw_airflow(&cli, args),
    }
}

fn schedules(cli: &Cli, args: &SchedulesArgs) -> Result<()> {
    let api = AirflowApi::new(cli)?;
    let response = api.get_value("dags?limit=100&order_by=dag_id")?;
    if args.full {
        return print_value(&response);
    }

    let schedules = response
        .get("dags")
        .and_then(|field| field.as_array())
        .context("Airflow response did not include dags array")?;
    print_line(format!(
        "{:<32} {:<7} {:<24} {:<14} {}",
        "schedule", "paused", "next_run", "timetable", "details"
    ))?;
    for schedule in schedules {
        let id = string_field(schedule, "dag_id");
        print_line(format!(
            "{:<32} {:<7} {:<24} {:<14} gairflow next-run --schedule {} --full",
            id,
            bool_field(schedule, "is_paused"),
            string_field(schedule, "next_dagrun_run_after"),
            string_field(schedule, "timetable_summary"),
            id,
        ))?;
    }
    Ok(())
}

fn list_runs(cli: &Cli, args: &RunsArgs) -> Result<()> {
    let api = AirflowApi::new(cli)?;
    let schedule = args.schedule.as_deref().unwrap_or("~");
    let mut url = api.url(&format!(
        "dags/{}/dagRuns?order_by=-start_date&limit=25",
        schedule
    ));
    if let Some(state) = &args.state {
        url.push_str("&state=");
        url.push_str(&urlencoding::encode(state));
    }
    let response = api.get_url_value(&url)?;
    if args.full {
        return print_value(&response);
    }
    print_runs_table(&response)
}

fn run(cli: &Cli, args: &RunArgs) -> Result<()> {
    let api = AirflowApi::new(cli)?;
    if args.tasks {
        return api.get(&format!(
            "dags/{}/dagRuns/{}/taskInstances",
            args.schedule, args.run_id
        ));
    }
    let run = api.get_value(&format!("dags/{}/dagRuns/{}", args.schedule, args.run_id))?;
    if args.full {
        return print_value(&run);
    }
    print_run_summary(&args.schedule, &run)
}

fn trigger(cli: &Cli, args: &TriggerArgs) -> Result<()> {
    let api = AirflowApi::new(cli)?;
    let body = if let Some(table) = &args.table {
        serde_json::json!({ "conf": { "table": table } })
    } else {
        serde_json::json!({ "conf": {} })
    };
    api.post(&format!("dags/{}/dagRuns", args.schedule), &body)
}

fn next_run(cli: &Cli, args: &NextRunArgs) -> Result<()> {
    let api = AirflowApi::new(cli)?;
    let schedule = api.get_value(&format!("dags/{}", args.schedule))?;
    if args.full {
        return print_value(&schedule);
    }
    print_value(&serde_json::json!({
        "schedule": args.schedule,
        "next_run": schedule.get("next_dagrun"),
        "next_run_data_interval_start": schedule.get("next_dagrun_data_interval_start"),
        "next_run_data_interval_end": schedule.get("next_dagrun_data_interval_end"),
        "is_paused": schedule.get("is_paused"),
    }))
}

#[derive(Args)]
struct TasksArgs {
    #[arg(long)]
    schedule: String,
}

fn tasks(cli: &Cli, args: &TasksArgs) -> Result<()> {
    let response = AirflowApi::new(cli)?.get_value(&format!("dags/{}/tasks", args.schedule))?;
    let tasks = response
        .get("tasks")
        .and_then(|value| value.as_array())
        .context("Airflow response did not include tasks array")?;

    print_line(format!(
        "{:<28} {:<24} {:<18} {:<10} {:<8} {}",
        "schedule", "task_id", "operator", "retries", "pool", "details"
    ))?;
    for task in tasks {
        let task_id = string_field(task, "task_id");
        let operator = string_field(task, "operator_name");
        let retries = number_field(task, "retries");
        let pool = string_field(task, "pool");
        print_line(format!(
            "{:<28} {:<24} {:<18} {:<10} {:<8} gairflow task {} --schedule {}",
            args.schedule, task_id, operator, retries, pool, task_id, args.schedule
        ))?;
    }
    Ok(())
}

fn task(cli: &Cli, task_id: &str) -> Result<()> {
    let schedule = match &cli.command {
        Commands::Task(args) => &args.schedule,
        _ => unreachable!("task() is only called for Commands::Task"),
    };
    let task = AirflowApi::new(cli)?.get_value(&format!("dags/{}/tasks/{}", schedule, task_id))?;
    print_value(&serde_json::json!({
        "schedule": schedule,
        "task": task,
    }))
}

fn patch_schedule(cli: &Cli, args: &ScheduleArgs, is_paused: bool) -> Result<()> {
    AirflowApi::new(cli)?.patch(
        &format!("dags/{}", args.schedule),
        &serde_json::json!({ "is_paused": is_paused }),
    )
}

struct AirflowApi {
    client: Client,
    token: String,
    base_url: String,
}

impl AirflowApi {
    fn new(cli: &Cli) -> Result<Self> {
        let client = Client::builder()
            .build()
            .context("failed to build HTTP client")?;
        let token = load_access_token(&client)?;
        let base_url = discover_airflow_url(&client, &token, cli)?
            .trim_end_matches('/')
            .to_string();
        Ok(Self {
            client,
            token,
            base_url,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/v2/{}", self.base_url, path.trim_start_matches('/'))
    }

    fn get(&self, path: &str) -> Result<()> {
        let value = self.get_value(path)?;
        print_value(&value)
    }

    fn get_url_value(&self, url: &str) -> Result<serde_json::Value> {
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .with_context(|| format!("GET {url} failed"))?;
        response_json(response)
    }

    fn get_value(&self, path: &str) -> Result<serde_json::Value> {
        let url = self.url(path);
        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .with_context(|| format!("GET {url} failed"))?;
        response_json(response)
    }

    fn post(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        let url = self.url(path);
        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .with_context(|| format!("POST {url} failed"))?;
        print_response(response)
    }

    fn patch(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        let url = self.url(path);
        let response = self
            .client
            .patch(&url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .with_context(|| format!("PATCH {url} failed"))?;
        print_response(response)
    }
}

fn discover_airflow_url(client: &Client, token: &str, cli: &Cli) -> Result<String> {
    let url = format!(
        "{COMPOSER_API}/projects/{}/locations/{}/environments/{}",
        cli.project, cli.location, cli.environment
    );
    let environment = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .with_context(|| format!("GET {url} failed"))?;
    let environment = response_json(environment)?;
    environment
        .pointer("/config/airflowUri")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .context("Composer environment response did not include config.airflowUri")
}

#[derive(Deserialize)]
struct AdcCredentials {
    #[serde(rename = "type")]
    kind: String,
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

fn load_access_token(client: &Client) -> Result<String> {
    if let Ok(token) = std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN") {
        if !token.trim().is_empty() {
            return Ok(token);
        }
    }

    let adc = read_adc_credentials()?;
    if adc.kind != "authorized_user" {
        bail!(
            "unsupported ADC credential type {}; set GOOGLE_OAUTH_ACCESS_TOKEN",
            adc.kind
        );
    }

    let response = client
        .post(TOKEN_URL)
        .form(&[
            ("client_id", adc.client_id.as_str()),
            ("client_secret", adc.client_secret.as_str()),
            ("refresh_token", adc.refresh_token.as_str()),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .context("OAuth token refresh request failed")?;
    let token = response_json(response)?;
    let token: TokenResponse =
        serde_json::from_value(token).context("OAuth token refresh response was malformed")?;
    Ok(token.access_token)
}

fn read_adc_credentials() -> Result<AdcCredentials> {
    let path = adc_path()?;
    let data = fs::read_to_string(&path)
        .with_context(|| format!("failed to read ADC credentials from {}", path.display()))?;
    serde_json::from_str(&data).context("ADC credentials are not valid JSON")
}

fn adc_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".config/gcloud/application_default_credentials.json"))
}

fn print_response(response: reqwest::blocking::Response) -> Result<()> {
    let value = response_json(response)?;
    print_value(&value)
}

fn response_json(response: reqwest::blocking::Response) -> Result<serde_json::Value> {
    let status = response.status();
    let text = response
        .text()
        .context("failed to read HTTP response body")?;
    if !status.is_success() {
        bail!("HTTP {status}\n{text}");
    }
    serde_json::from_str(&text).context("HTTP response was not valid JSON")
}

fn print_value(value: &serde_json::Value) -> Result<()> {
    print_line(serde_json::to_string_pretty(value)?)
}

fn print_line(line: String) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    if let Err(error) = writeln!(stdout, "{line}") {
        if error.kind() == ErrorKind::BrokenPipe {
            std::process::exit(0);
        }
        return Err(error).context("failed to write output");
    }
    Ok(())
}

fn print_runs_table(value: &serde_json::Value) -> Result<()> {
    let runs = value
        .get("dag_runs")
        .and_then(|field| field.as_array())
        .context("Airflow response did not include dag_runs array")?;
    print_line(format!(
        "{:<28} {:<8} {:<36} {:<20} {}",
        "schedule", "state", "run_after", "duration", "run_id"
    ))?;
    for run in runs {
        print_line(format!(
            "{:<28} {:<8} {:<36} {:<20} {}",
            string_field(run, "dag_id"),
            string_field(run, "state"),
            string_field(run, "run_after"),
            duration_field(run),
            string_field(run, "dag_run_id"),
        ))?;
    }
    Ok(())
}

fn print_run_summary(schedule: &str, run: &serde_json::Value) -> Result<()> {
    print_value(&serde_json::json!({
        "schedule": schedule,
        "dag_run_id": run.get("dag_run_id"),
        "state": run.get("state"),
        "run_type": run.get("run_type"),
        "run_after": run.get("run_after"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "duration": run.get("duration"),
        "conf": run.get("conf"),
        "tasks": format!("gairflow run {} --schedule {} --tasks", string_field(run, "dag_run_id"), schedule),
        "full": format!("gairflow run {} --schedule {} --full", string_field(run, "dag_run_id"), schedule),
    }))
}

fn duration_field(value: &serde_json::Value) -> String {
    value
        .get("duration")
        .map(|field| match field {
            serde_json::Value::Null => "-".to_string(),
            serde_json::Value::Number(number) => number.to_string(),
            serde_json::Value::String(text) => text.clone(),
            _ => "-".to_string(),
        })
        .unwrap_or_else(|| "-".to_string())
}

fn string_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|field| field.as_str())
        .unwrap_or("-")
        .to_string()
}

fn number_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .map(|field| match field {
            serde_json::Value::Number(number) => number.to_string(),
            serde_json::Value::String(text) => text.clone(),
            _ => "-".to_string(),
        })
        .unwrap_or_else(|| "-".to_string())
}

fn bool_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|field| field.as_bool())
        .map(|field| field.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn raw_airflow(cli: &Cli, args: &RawArgs) -> Result<()> {
    if args.args.is_empty() {
        bail!("raw requires Airflow CLI arguments");
    }
    let status = composer_base_command(cli)
        .args(&args.args)
        .status()
        .context("failed to run gcloud composer command")?;
    exit_from_status(status)
}

fn composer_base_command(cli: &Cli) -> Command {
    let mut cmd = Command::new("gcloud");
    cmd.args([
        "composer",
        "environments",
        "run",
        &cli.environment,
        "--location",
        &cli.location,
    ]);
    cmd
}

fn logs(cli: &Cli, args: &LogsArgs) -> Result<()> {
    let filter = build_log_filter(&cli.project, &args)?;
    let status = Command::new("gcloud")
        .args([
            "logging",
            "read",
            &filter,
            "--project",
            &cli.project,
            "--limit",
            &args.limit.to_string(),
            "--format",
            "value(timestamp,textPayload)",
        ])
        .status()
        .context("failed to run gcloud logging read")?;
    exit_from_status(status)
}

fn build_log_filter(project: &str, args: &LogsArgs) -> Result<String> {
    let mut parts = vec![
        "resource.type=\"cloud_composer_environment\"".to_string(),
        "resource.labels.environment_name=\"gc-composer\"".to_string(),
        format!("timestamp>={}", log_since_expr(&args.since)?),
    ];
    if let Some(run_id) = &args.run_id {
        parts.push(format!("textPayload:\"{}\"", escape_log_value(run_id)));
    }
    if let Some(schedule) = &args.schedule {
        parts.push(format!("textPayload:\"{}\"", escape_log_value(schedule)));
    }
    if let Some(table) = &args.table {
        parts.push(format!("textPayload:\"{}\"", escape_log_value(table)));
    }
    if args.run_id.is_none() && args.schedule.is_none() && args.table.is_none() {
        parts.push(format!("resource.labels.project_id=\"{}\"", project));
    }
    Ok(parts.join("\nAND "))
}

fn log_since_expr(since: &str) -> Result<String> {
    if since.contains('T') {
        Ok(format!("\"{}\"", since))
    } else if let Some(hours) = since.strip_suffix('h') {
        let hours: i64 = hours.parse().context("invalid --since hours")?;
        Ok(format!("\"{}\"", chrono_like_hours_ago(hours)?))
    } else {
        bail!("--since must be an RFC3339 timestamp or an hour duration like 24h")
    }
}

fn chrono_like_hours_ago(hours: i64) -> Result<String> {
    let output = Command::new("date")
        .args([
            "-u",
            "-d",
            &format!("{hours} hours ago"),
            "+%Y-%m-%dT%H:%M:%SZ",
        ])
        .output()
        .context("failed to compute timestamp with date")?;
    if !output.status.success() {
        bail!("date failed while computing --since");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn escape_log_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn config_check(uri: &str) -> Result<()> {
    let output = Command::new("gcloud")
        .args(["storage", "cat", uri])
        .output()
        .with_context(|| format!("failed to read {uri}"))?;
    if !output.status.success() {
        std::io::stderr().write_all(&output.stderr).ok();
        bail!("failed to read live config");
    }
    let config: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("live config is not valid JSON")?;
    let include_tables = array_of_strings(&config, "include_tables");
    let expected_skip_tables = array_of_strings(&config, "expected_skip_tables");
    let bootstrap_acknowledged = array_of_strings(&config, "bootstrap_acknowledged");

    let mut ok = true;
    for table in ["income", "income_content_summary", "utms"] {
        let included = include_tables.iter().any(|item| item == table);
        print_line(format!("{table}_included={included}"))?;
        ok &= !included;
    }
    for table in [
        "sessions",
        "paid_users_pages",
        "paid_subscriptions_users_pages",
    ] {
        let skipped = expected_skip_tables.iter().any(|item| item == table);
        print_line(format!("{table}_skipped={skipped}"))?;
        ok &= skipped;
    }
    print_line(format!("bootstrap_acknowledged={bootstrap_acknowledged:?}"))?;
    ok &= bootstrap_acknowledged.is_empty();

    if !ok {
        bail!("live config is not restored to safe normal-sync state");
    }
    Ok(())
}

fn array_of_strings(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(|raw| raw.as_array())
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(ToOwned::to_owned))
        .collect()
}

fn exit_from_status(status: std::process::ExitStatus) -> Result<()> {
    if status.success() {
        Ok(())
    } else {
        std::process::exit(status.code().unwrap_or(1));
    }
}
