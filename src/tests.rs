mod mock;

use super::*;
use mock::{MockAirflowServer, run_json, unique_temp_dir, write_fake_gcloud};
use std::sync::{Mutex, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

struct EnvGuard {
    composer_api: Option<String>,
    token: Option<String>,
    token_url: Option<String>,
    home: Option<String>,
    path: Option<String>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        restore_env(COMPOSER_API_ENV, &self.composer_api);
        restore_env("GOOGLE_OAUTH_ACCESS_TOKEN", &self.token);
        restore_env(TOKEN_URL_ENV, &self.token_url);
        restore_env("HOME", &self.home);
        restore_env("PATH", &self.path);
    }
}

fn restore_env(key: &str, value: &Option<String>) {
    unsafe {
        if let Some(value) = value {
            std::env::set_var(key, value);
        } else {
            std::env::remove_var(key);
        }
    }
}

fn with_mock_environment(server: &MockAirflowServer, test: impl FnOnce()) {
    let _lock = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let guard = EnvGuard {
        composer_api: std::env::var(COMPOSER_API_ENV).ok(),
        token: std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok(),
        token_url: std::env::var(TOKEN_URL_ENV).ok(),
        home: std::env::var("HOME").ok(),
        path: std::env::var("PATH").ok(),
    };
    unsafe {
        std::env::set_var(COMPOSER_API_ENV, &server.base_url);
        std::env::set_var("GOOGLE_OAUTH_ACCESS_TOKEN", "test-token");
    }

    test();
    drop(guard);
}

#[test]
fn parses_cli_commands_and_defaults() {
    let cli = Cli::try_parse_from(["gairflow", "schedules"]).unwrap();
    assert_eq!(cli.environment, DEFAULT_ENVIRONMENT);
    assert_eq!(cli.location, DEFAULT_LOCATION);
    assert_eq!(cli.project, DEFAULT_PROJECT);
    assert!(matches!(
        cli.command,
        Commands::Schedules(SchedulesArgs { full: false })
    ));

    let cli = Cli::try_parse_from([
        "gairflow",
        "--project",
        "gc-dev",
        "logs",
        "--schedule",
        "sync",
        "--run-id",
        "run-1",
        "--table",
        "pages",
        "--since",
        "2026-06-23T00:00:00Z",
        "--limit",
        "5",
    ])
    .unwrap();
    assert_eq!(cli.project, "gc-dev");
    assert!(matches!(
        cli.command,
        Commands::Logs(LogsArgs {
            schedule: Some(schedule),
            run_id: Some(run_id),
            table: Some(table),
            since,
            limit: 5,
        }) if schedule == "sync"
            && run_id == "run-1"
            && table == "pages"
            && since == "2026-06-23T00:00:00Z"
    ));
}

#[test]
fn airflow_api_urls_trim_slashes() {
    let api = AirflowApi {
        client: Client::new(),
        token: "token".to_string(),
        base_url: "http://example.test/root".to_string(),
    };

    assert_eq!(
        api.url("/dags/sync"),
        "http://example.test/root/api/v2/dags/sync"
    );
}

#[test]
fn airflow_api_reports_bad_json() {
    let server = MockAirflowServer::start();
    let api = AirflowApi {
        client: Client::new(),
        token: "token".to_string(),
        base_url: format!("{}/airflow", server.base_url),
    };

    let err = api.get_value("invalid-json").unwrap_err();
    assert!(err.to_string().contains("HTTP response was not valid JSON"));

    let err = api.get_value("status-error").unwrap_err();
    assert!(err.to_string().contains("HTTP 500 Internal Server Error"));
}

#[test]
fn airflow_commands_use_discovered_api() {
    let server = MockAirflowServer::start();
    with_mock_environment(&server, || {
        let cli = cli_for(Commands::Schedules(SchedulesArgs { full: false }));
        schedules(&cli, match_command_schedules(&cli)).unwrap();

        let cli = cli_for(Commands::Schedules(SchedulesArgs { full: true }));
        schedules(&cli, match_command_schedules(&cli)).unwrap();

        let cli = cli_for(Commands::Runs(RunsArgs {
            schedule: Some("sync".to_string()),
            state: Some("success".to_string()),
            full: false,
        }));
        list_runs(&cli, match_command_runs(&cli)).unwrap();

        let cli = cli_for(Commands::Runs(RunsArgs {
            schedule: None,
            state: None,
            full: true,
        }));
        list_runs(&cli, match_command_runs(&cli)).unwrap();

        let cli = cli_for(Commands::Run(RunArgs {
            run_id: "run-1".to_string(),
            schedule: "sync".to_string(),
            tasks: true,
            full: false,
        }));
        run(&cli, match_command_run(&cli)).unwrap();

        let cli = cli_for(Commands::Run(RunArgs {
            run_id: "run-1".to_string(),
            schedule: "sync".to_string(),
            tasks: false,
            full: true,
        }));
        run(&cli, match_command_run(&cli)).unwrap();

        let cli = cli_for(Commands::Run(RunArgs {
            run_id: "run-1".to_string(),
            schedule: "sync".to_string(),
            tasks: false,
            full: false,
        }));
        run(&cli, match_command_run(&cli)).unwrap();

        let cli = cli_for(Commands::NextRun(NextRunArgs {
            schedule: "sync".to_string(),
            full: false,
        }));
        next_run(&cli, match_command_next_run(&cli)).unwrap();

        let cli = cli_for(Commands::NextRun(NextRunArgs {
            schedule: "sync".to_string(),
            full: true,
        }));
        next_run(&cli, match_command_next_run(&cli)).unwrap();

        let cli = cli_for(Commands::Tasks(TasksArgs {
            schedule: "sync".to_string(),
        }));
        tasks(&cli, match_command_tasks(&cli)).unwrap();

        let cli = cli_for(Commands::Task(TaskArgs {
            task_id: "task-1".to_string(),
            schedule: "sync".to_string(),
        }));
        task(&cli, "task-1").unwrap();

        let cli = cli_for(Commands::Pause(ScheduleArgs {
            schedule: "sync".to_string(),
        }));
        patch_schedule(&cli, match_command_schedule(&cli), true).unwrap();

        let cli = cli_for(Commands::Unpause(ScheduleArgs {
            schedule: "sync".to_string(),
        }));
        patch_schedule(&cli, match_command_schedule(&cli), false).unwrap();

        let cli = cli_for(Commands::Trigger(TriggerArgs {
            schedule: "sync".to_string(),
            table: Some("pages".to_string()),
        }));
        trigger(&cli, match_command_trigger(&cli)).unwrap();

        let cli = cli_for(Commands::Trigger(TriggerArgs {
            schedule: "sync".to_string(),
            table: None,
        }));
        trigger(&cli, match_command_trigger(&cli)).unwrap();
    });

    let requests = server.requests();
    assert!(requests.iter().any(|request| {
        request.starts_with("GET /airflow/api/v2/dags?limit=100&order_by=dag_id")
    }));
    assert!(requests.iter().any(|request| {
        request.starts_with(
            "GET /airflow/api/v2/dags/sync/dagRuns?order_by=-start_date&limit=25&state=success",
        )
    }));
    assert!(requests.iter().any(|request| {
        request.contains("PATCH /airflow/api/v2/dags/sync")
            && request.contains(r#""is_paused":true"#)
    }));
    assert!(requests.iter().any(|request| {
        request.contains("POST /airflow/api/v2/dags/sync/dagRuns")
            && request.contains(r#""table":"pages"#)
    }));
}

#[test]
fn dispatch_routes_mocked_commands() {
    let server = MockAirflowServer::start();
    with_mock_environment(&server, || {
        for cli in [
            cli_for(Commands::Schedules(SchedulesArgs { full: false })),
            cli_for(Commands::Runs(RunsArgs {
                schedule: Some("sync".to_string()),
                state: None,
                full: false,
            })),
            cli_for(Commands::Run(RunArgs {
                run_id: "run-1".to_string(),
                schedule: "sync".to_string(),
                tasks: false,
                full: false,
            })),
            cli_for(Commands::NextRun(NextRunArgs {
                schedule: "sync".to_string(),
                full: false,
            })),
            cli_for(Commands::Tasks(TasksArgs {
                schedule: "sync".to_string(),
            })),
            cli_for(Commands::Task(TaskArgs {
                task_id: "task-1".to_string(),
                schedule: "sync".to_string(),
            })),
            cli_for(Commands::Pause(ScheduleArgs {
                schedule: "sync".to_string(),
            })),
            cli_for(Commands::Unpause(ScheduleArgs {
                schedule: "sync".to_string(),
            })),
            cli_for(Commands::Trigger(TriggerArgs {
                schedule: "sync".to_string(),
                table: None,
            })),
        ] {
            dispatch(&cli).unwrap();
        }
    });
}

#[test]
fn formatting_helpers_cover_missing_and_typed_values() {
    let value = serde_json::json!({
        "text": "value",
        "number": 42,
        "number_text": "12",
        "duration": null,
        "duration_text": "1.25",
        "flag": true,
        "strings": ["a", 7, "b"]
    });

    assert_eq!(string_field(&value, "text"), "value");
    assert_eq!(string_field(&value, "missing"), "-");
    assert_eq!(number_field(&value, "number"), "42");
    assert_eq!(number_field(&value, "number_text"), "12");
    assert_eq!(number_field(&value, "text"), "value");
    assert_eq!(number_field(&value, "flag"), "-");
    assert_eq!(duration_field(&value), "-");
    assert_eq!(
        duration_field(&serde_json::json!({ "duration": "1.25" })),
        "1.25"
    );
    assert_eq!(duration_field(&serde_json::json!({ "duration": 3 })), "3");
    assert_eq!(
        duration_field(&serde_json::json!({ "duration": { "bad": true } })),
        "-"
    );
    assert_eq!(bool_field(&value, "flag"), "true");
    assert_eq!(bool_field(&value, "missing"), "-");
    assert_eq!(array_of_strings(&value, "strings"), vec!["a", "b"]);
    assert!(print_runs_table(&serde_json::json!({ "dag_runs": [] })).is_ok());
    assert!(print_runs_table(&serde_json::json!({})).is_err());
    assert!(print_run_summary("sync", &run_json()).is_ok());
}

#[test]
fn log_filter_escapes_and_validates_since() {
    let args = LogsArgs {
        schedule: Some("sync\"dag".to_string()),
        run_id: Some("run\\1".to_string()),
        table: Some("pages".to_string()),
        since: "2026-06-23T00:00:00Z".to_string(),
        limit: 10,
    };
    let filter = build_log_filter("globalcomix", &args).unwrap();
    assert!(filter.contains("timestamp>=\"2026-06-23T00:00:00Z\""));
    assert!(filter.contains("textPayload:\"run\\\\1\""));
    assert!(filter.contains("textPayload:\"sync\\\"dag\""));
    assert!(!filter.contains("resource.labels.project_id"));

    let fallback = build_log_filter(
        "globalcomix",
        &LogsArgs {
            schedule: None,
            run_id: None,
            table: None,
            since: "2026-06-23T00:00:00Z".to_string(),
            limit: 10,
        },
    )
    .unwrap();
    assert!(fallback.contains("resource.labels.project_id=\"globalcomix\""));

    assert!(log_since_expr("bad").is_err());
    assert!(log_since_expr("not-hours h").is_err());
    assert!(log_since_expr("0h").unwrap().contains('T'));
    assert_eq!(escape_log_value("a\\b\"c"), "a\\\\b\\\"c");
}

#[test]
fn config_state_accepts_safe_config_and_rejects_unsafe_config() {
    let safe = serde_json::json!({
        "include_tables": ["pages"],
        "expected_skip_tables": [
            "sessions",
            "paid_users_pages",
            "paid_subscriptions_users_pages"
        ],
        "bootstrap_acknowledged": []
    });
    check_config_state(&safe).unwrap();

    let unsafe_config = serde_json::json!({
        "include_tables": ["income"],
        "expected_skip_tables": [],
        "bootstrap_acknowledged": ["income"]
    });
    let err = check_config_state(&unsafe_config).unwrap_err();
    assert!(
        err.to_string()
            .contains("live config is not restored to safe normal-sync state")
    );
}

#[test]
fn access_token_uses_non_empty_environment_value() {
    let server = MockAirflowServer::start();
    with_mock_environment(&server, || {
        let token = load_access_token(&Client::new()).unwrap();
        assert_eq!(token, "test-token");
    });
}

#[test]
fn access_token_refreshes_from_adc_credentials() {
    let server = MockAirflowServer::start();
    let _lock = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let guard = EnvGuard {
        composer_api: std::env::var(COMPOSER_API_ENV).ok(),
        token: std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok(),
        token_url: std::env::var(TOKEN_URL_ENV).ok(),
        home: std::env::var("HOME").ok(),
        path: std::env::var("PATH").ok(),
    };
    let home = unique_temp_dir("gairflow-adc");
    std::fs::create_dir_all(home.join(".config/gcloud")).unwrap();
    std::fs::write(
        home.join(".config/gcloud/application_default_credentials.json"),
        serde_json::json!({
            "type": "authorized_user",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "refresh_token": "refresh-token"
        })
        .to_string(),
    )
    .unwrap();
    unsafe {
        std::env::set_var("HOME", &home);
        std::env::set_var("GOOGLE_OAUTH_ACCESS_TOKEN", "");
        std::env::set_var(TOKEN_URL_ENV, format!("{}/token", server.base_url));
    }

    assert_eq!(
        adc_path().unwrap(),
        home.join(".config/gcloud/application_default_credentials.json")
    );
    let adc = read_adc_credentials().unwrap();
    assert_eq!(adc.kind, "authorized_user");
    assert_eq!(
        load_access_token(&Client::new()).unwrap(),
        "refreshed-token"
    );
    assert!(server.requests().iter().any(|request| {
        request.starts_with("POST /token") && request.contains("refresh-token")
    }));

    drop(guard);
    std::fs::remove_dir_all(home).ok();
}

#[test]
fn access_token_rejects_unsupported_adc_type() {
    let _lock = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let guard = EnvGuard {
        composer_api: std::env::var(COMPOSER_API_ENV).ok(),
        token: std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok(),
        token_url: std::env::var(TOKEN_URL_ENV).ok(),
        home: std::env::var("HOME").ok(),
        path: std::env::var("PATH").ok(),
    };
    let home = unique_temp_dir("gairflow-adc-bad");
    std::fs::create_dir_all(home.join(".config/gcloud")).unwrap();
    std::fs::write(
        home.join(".config/gcloud/application_default_credentials.json"),
        serde_json::json!({
            "type": "service_account",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "refresh_token": "refresh-token"
        })
        .to_string(),
    )
    .unwrap();
    unsafe {
        std::env::set_var("HOME", &home);
        std::env::remove_var("GOOGLE_OAUTH_ACCESS_TOKEN");
    }

    let err = load_access_token(&Client::new()).unwrap_err();
    assert!(err.to_string().contains("unsupported ADC credential type"));

    drop(guard);
    std::fs::remove_dir_all(home).ok();
}

#[test]
fn raw_and_composer_helpers_validate_without_running_gcloud() {
    let cli = cli_for(Commands::Raw(RawArgs { args: Vec::new() }));
    let args = match &cli.command {
        Commands::Raw(args) => args,
        _ => unreachable!(),
    };
    let err = raw_airflow(&cli, args).unwrap_err();
    assert!(
        err.to_string()
            .contains("raw requires Airflow CLI arguments")
    );

    let mut cli = cli_for(Commands::Schedules(SchedulesArgs { full: false }));
    cli.environment = "env".to_string();
    cli.location = "loc".to_string();
    let command = composer_base_command(&cli);
    assert_eq!(command.get_program(), "gcloud");
    assert_eq!(
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>(),
        vec![
            "composer",
            "environments",
            "run",
            "env",
            "--location",
            "loc"
        ]
    );

    let status = Command::new("true").status().unwrap();
    assert!(exit_from_status(status).is_ok());
}

#[test]
fn gcloud_wrappers_run_against_fake_gcloud() {
    let _lock = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let guard = EnvGuard {
        composer_api: std::env::var(COMPOSER_API_ENV).ok(),
        token: std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok(),
        token_url: std::env::var(TOKEN_URL_ENV).ok(),
        home: std::env::var("HOME").ok(),
        path: std::env::var("PATH").ok(),
    };
    let bin_dir = unique_temp_dir("gairflow-bin");
    write_fake_gcloud(&bin_dir);
    unsafe {
        std::env::set_var("PATH", &bin_dir);
    }

    config_check("gs://config").unwrap();

    let logs_cli = cli_for(Commands::Logs(LogsArgs {
        schedule: None,
        run_id: None,
        table: None,
        since: "2026-06-23T00:00:00Z".to_string(),
        limit: 1,
    }));
    let logs_args = match &logs_cli.command {
        Commands::Logs(args) => args,
        _ => unreachable!(),
    };
    logs(&logs_cli, logs_args).unwrap();
    dispatch(&logs_cli).unwrap();

    let raw_cli = cli_for(Commands::Raw(RawArgs {
        args: vec![OsString::from("dags"), OsString::from("list")],
    }));
    let raw_args = match &raw_cli.command {
        Commands::Raw(args) => args,
        _ => unreachable!(),
    };
    raw_airflow(&raw_cli, raw_args).unwrap();
    dispatch(&raw_cli).unwrap();

    let config_cli = cli_for(Commands::ConfigCheck(ConfigCheckArgs {
        uri: "gs://config".to_string(),
    }));
    dispatch(&config_cli).unwrap();

    drop(guard);
    std::fs::remove_dir_all(bin_dir).ok();
}

fn cli_for(command: Commands) -> Cli {
    Cli {
        environment: DEFAULT_ENVIRONMENT.to_string(),
        location: DEFAULT_LOCATION.to_string(),
        project: DEFAULT_PROJECT.to_string(),
        command,
    }
}

fn match_command_schedules(cli: &Cli) -> &SchedulesArgs {
    match &cli.command {
        Commands::Schedules(args) => args,
        _ => unreachable!(),
    }
}

fn match_command_runs(cli: &Cli) -> &RunsArgs {
    match &cli.command {
        Commands::Runs(args) => args,
        _ => unreachable!(),
    }
}

fn match_command_run(cli: &Cli) -> &RunArgs {
    match &cli.command {
        Commands::Run(args) => args,
        _ => unreachable!(),
    }
}

fn match_command_next_run(cli: &Cli) -> &NextRunArgs {
    match &cli.command {
        Commands::NextRun(args) => args,
        _ => unreachable!(),
    }
}

fn match_command_tasks(cli: &Cli) -> &TasksArgs {
    match &cli.command {
        Commands::Tasks(args) => args,
        _ => unreachable!(),
    }
}

fn match_command_schedule(cli: &Cli) -> &ScheduleArgs {
    match &cli.command {
        Commands::Pause(args) | Commands::Unpause(args) => args,
        _ => unreachable!(),
    }
}

fn match_command_trigger(cli: &Cli) -> &TriggerArgs {
    match &cli.command {
        Commands::Trigger(args) => args,
        _ => unreachable!(),
    }
}
