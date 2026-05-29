# Sinai Call Agent

Telegram admin bot for read-only amoCRM call analysis.

## What it does

- Reads amoCRM only with `GET` requests.
- Looks at the sales pipeline and the legal department pipeline.
- Uses all stages in selected pipelines.
- Uses only these managers:
  - Павел
  - Ольга Шевелева
  - Дегтярева Юлия
  - Юлия Миллер
- Takes calls with recording links and duration from 5 minutes.
- Transcribes audio through OpenAI.
- Analyzes transcript through OpenAI.
- Sends the report to the Telegram admin.
- Stores local state in SQLite to avoid repeated processing.
- Keeps a Telegram archive of completed analyses, split by manager, with search by amoCRM lead ID.

The bot does not write notes, fields, tags, statuses, or anything else to amoCRM.

## Run

From this folder:

```powershell
pip install -r requirements.txt
python .\agent.py
```

The script reads `.env` from this folder first, then from the parent project folder.

Required env vars:

- `TG_BOT_TOKEN`
- `ADMIN_ID`
- `AMOCRM_BASE_URL`
- `AMOCRM_ACCESS_TOKEN`
- `OPENAI_API_KEY`

Optional env vars:

- `LOG_LEVEL`, default `DEBUG`
- `OPENAI_TRANSCRIBE_MODEL`, default `gpt-4o-mini-transcribe`
- `OPENAI_ANALYSIS_MODEL`, default `gpt-4o-mini`
- `GROQ_TRANSCRIBE_MODEL`, default `whisper-large-v3-turbo`
- `GROQ_ANALYSIS_MODEL`, default `llama-3.3-70b-versatile`
- `FREELLM_BASE_URL`, default `http://155.212.217.115:3001/v1`
- `FREELLM_ANALYSIS_MODEL`, default falls back to `FREELLM_MODEL`, `LLM_MODEL`, then `llama-3.3-70b-versatile`
- `CALL_MIN_DURATION_SECONDS`, default `300`
- `MONITOR_INTERVAL_SECONDS`, default `300`
- `AMOCRM_SALES_PIPELINE_ID`, default `867829`
- `AMOCRM_LEGAL_PIPELINE_ID`, default `1312204`
- `AMOCRM_PIPELINE_IDS`, optional comma-separated override for target pipelines
- `AUDIO_PROXY_URL`, optional proxy only for mp3 downloads, supports `http://`, `https://`, `socks5://`
- `PROXY_URL`, optional global proxy for all HTTP requests, supports `http://`, `https://`, `socks5://`
- `AUDIO_HTTP_PROXY`, `AUDIO_HTTPS_PROXY`, `HTTP_PROXY`, `HTTPS_PROXY`, optional split proxy settings

## AI Modes

The Telegram panel can switch between two modes:

- Test mode: transcription through `GROQ_API_KEY`; analysis first tries `GROQ_API_KEY`, then falls back to `FREELLM_API_KEY`.
- Paid mode: transcription and analysis through `OPENAI_API_KEY`.

## Telegram Panel

Use `/start` in Telegram:

- `Анализ сегодня`
- `Анализ вчера`
- `Анализ сегодня+вчера`
- `Показать найденные звонки`
- `Старт мониторинга`
- `Стоп мониторинга`
- `Статистика`
- `Состояние`
- `База анализов`
- `Тестовый режим`
- `Платный режим`

## Logs

Console logs are detailed by default and also written to `data/logs/agent_YYYY-MM-DD.log`.
They show:

- amoCRM endpoints and response sizes
- allowed sales statuses
- accepted lead count
- every inspected call note
- rejection reasons, such as wrong date, short duration, no recording, already processed
- audio download, transcription, analysis provider, and saved file paths
