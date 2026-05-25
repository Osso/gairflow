# gairflow

Direct GlobalComix Cloud Composer/Airflow helper for schedules, runs, and tasks.

Normal commands use Composer and Airflow REST APIs directly. They do not run
`gcloud composer environments run` and do not create Composer CLI execution IDs.

Defaults:

- Composer environment: `gc-composer`
- Location: `us-central1`
- Project: `globalcomix`

Examples:

```bash
gairflow schedules
gairflow schedules --full
gairflow unpause --schedule mysql_to_bq_raw_sync
gairflow next-run --schedule mysql_to_bq_raw_sync
gairflow next-run --schedule mysql_to_bq_raw_sync --full
gairflow runs
gairflow runs --schedule mysql_to_bq_raw_sync
gairflow runs --schedule mysql_to_bq_raw_sync --state running
gairflow run 'manual__2026-05-25T07:04:32.029402+00:00_nB9ETN1G' --schedule mysql_to_bq_raw_sync
gairflow run 'manual__2026-05-25T07:04:32.029402+00:00_nB9ETN1G' --schedule mysql_to_bq_raw_sync --full
gairflow run 'manual__2026-05-25T07:04:32.029402+00:00_nB9ETN1G' --schedule mysql_to_bq_raw_sync --tasks
gairflow tasks --schedule mysql_to_bq_raw_sync
gairflow task sync_raw_tables --schedule mysql_to_bq_raw_sync
gairflow trigger --schedule mysql_to_bq_raw_sync
gairflow trigger --schedule mysql_to_bq_raw_sync --table paid_subscriptions_users_pages
gairflow logs --run-id 'manual__2026-05-25T07:04:32.029402+00:00_nB9ETN1G'
gairflow config-check
```

Authentication uses `~/.config/gcloud/application_default_credentials.json` or
`GOOGLE_OAUTH_ACCESS_TOKEN`.

Use `gairflow raw -- <airflow args...>` only when you explicitly want the old
Composer CLI wrapper behavior.
