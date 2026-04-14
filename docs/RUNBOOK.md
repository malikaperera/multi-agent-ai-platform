# Roderick - Runbook

## Starting the platform

### Docker (recommended)

```bash
git clone <your-repo-url>
cd multi-agent-ai-platform

cp .env.example .env
# Edit .env - fill in TELEGRAM_BOT_TOKEN, ANTHROPIC_API_KEY, AUTHORIZED_CHAT_ID

docker compose up -d

# Logs
docker compose logs -f

# Stop
docker compose down
```

On first startup, Roderick sends "Roderick online. Ready." to your Telegram.

### Direct (development)

```bash
pip install -r requirements.txt

cp .env.example .env
# Edit .env

python apps/roderick/main.py
```

---

## Dashboard and API

```bash
# Full stack via Docker (recommended)
docker compose up -d

# Development: run API and dashboard separately
uvicorn apps.api.main:app --host 0.0.0.0 --port 8000
cd dashboard && npm install && npm run dev
```

Open:
- Dashboard: `http://localhost:3000`
- API: `http://localhost:8000`
- Langfuse observability: `http://localhost:3001`

The dashboard provides full visibility into agent queues, task pipelines, approvals, Zuko job applications, Venture opportunities, Atlas lessons, and system events.

---

## Telegram commands

| Command | Effect |
|---|---|
| `/start` | Capability list |
| `/brief` | On-demand morning briefing (reminders + agent status) |
| `/agents` | Agent registry with status and current task |
| `/pending` | List pending approval requests |
| `/tasks` | Task queue overview |
| `/zuko` | Job search status summary |
| `/clear` | Clear conversation history |

---

## How to create reminders

Just talk to Roderick:

```
remind me to submit the invoice on Friday
set a reminder about the team sync tomorrow at 2pm
note: follow up with the client about the proposal next week
```

Roderick classifies the message and saves the reminder. Use `/brief` to see upcoming reminders.

---

## How task routing works

Roderick classifies every incoming message:

| Category | What happens |
|---|---|
| `reminder_planning` | Handled directly - save/list reminders |
| `personal_admin` | Handled directly by Roderick |
| `direct_answer` | Answered via agentic LLM loop |
| `agent_status` | Returns agent registry summary |
| `research` | Queued for Merlin |
| `opportunity` | Queued for Venture |
| `build` | Approval keyboard sent; Forge plans after approval |

---

## Approval workflow

For build requests and high-capital opportunities, Roderick sends an inline keyboard:

```
✅ Approve  ❌ Reject
⏸ Defer    ❓ Ask details
```

**Forge flow:**
1. Owner approves the task → Forge creates an implementation plan
2. Plan is presented → owner approves the plan
3. Forge builds → Sentinel validates → promoted to live

Forge never creates files before plan approval. Use `/pending` to see outstanding approvals.

**Venture approvals** are triggered when an opportunity exceeds the configured capital threshold.

---

## Configuring the system

Edit `config/roderick.json` for all tuneable settings. Key sections:

```json
{
  "llm": {
    "provider": "anthropic",
    "model": "claude-sonnet-4-6"
  },
  "scheduler": {
    "timezone": "UTC",
    "daily_briefing": "08:00",
    "atlas_daily_lesson": "09:00"
  },
  "zuko": {
    "proactive_scans": {
      "enabled": true,
      "interval_hours": 3
    }
  },
  "atlas": {
    "curriculum_focus": ["Python", "Docker", "Kubernetes"]
  },
  "venture": {
    "capital_guardrails": {
      "approval_threshold": 500,
      "currency": "USD"
    }
  }
}
```

Changes take effect on restart (config is loaded once at startup).

---

## Configuring Zuko (job search)

**Job targeting**: edit `apps/zuko/config/job_targeting.json`:
- `target_location`: city/region for job searches
- `target_roles`: list of role titles to scan
- `high_fit_keywords`: scoring keywords for strong matches
- `exclusions`: keywords that disqualify a role

**Candidate profile**: create from the example:
```bash
cp apps/zuko/config/candidate_profile.example.json apps/zuko/config/candidate_profile.json
# Edit with your CV summary and cover letter preferences
```

**CV for auto-apply**: place your CV PDF at the path set in `.env`:
```
CV_DIR=/path/to/your/cv/directory
```

---

## Configuring Atlas (skill tutor)

Set your learning domain in `config/roderick.json`:

```json
"atlas": {
  "curriculum_focus": ["Python", "Docker", "Terraform", "Data Engineering"]
}
```

Atlas generates daily structured lessons on these topics, tracks your skill progression, and uses your real Roderick system as the learning environment. Talk to Atlas via its dedicated Telegram chat or the dashboard.

---

## Configuring Venture (business opportunities)

Set your venture domains and capital guardrails in `config/roderick.json`:

```json
"venture": {
  "proactive_research_domains": [
    "Your first venture: market keyword gaps, lead signals, route-to-cash",
    "Your second venture: competitor weaknesses, pricing gaps, user needs"
  ],
  "capital_guardrails": {
    "free_threshold": 100,
    "light_review_threshold": 500,
    "approval_threshold": 3000,
    "currency": "USD"
  }
}
```

---

## Owner memory files

Files in `memory/` are plain markdown - edit them directly to update what all agents know about you:

| File | Purpose |
|---|---|
| `owner_profile.md` | Your role, goals, and background |
| `current_focus.md` | What you are actively working on |
| `active_projects.md` | Projects and their current status |
| `capital_state.md` | Financial context for Venture decisions |
| `job_market.md` | Job market data used by Atlas and Zuko |
| `opportunity_log.md` | Opportunity history |

Changes are picked up on the next agent task (memory is loaded fresh each time).

---

## Debugging common issues

**Bot doesn't start:**
- Check `.env` has `TELEGRAM_BOT_TOKEN`, `ANTHROPIC_API_KEY`, and `AUTHORIZED_CHAT_ID`
- Check `config/roderick.json` exists
- Run `python apps/roderick/main.py` directly for a full traceback

**"Roderick online." not received:**
- Verify `AUTHORIZED_CHAT_ID` matches your Telegram chat ID
  (send `/start` to `@userinfobot` on Telegram to get your ID)
- Check `logs/roderick.log`

**Agents not picking up tasks:**
```bash
sqlite3 data/roderick.db "SELECT id, to_agent, status, description FROM tasks ORDER BY created_at DESC LIMIT 20;"
```
Worker logs are in `logs/roderick.log`: search for `"processing task"`.

**Approval buttons not responding:**
```bash
sqlite3 data/roderick.db "SELECT id, status, callback_data FROM approval_requests ORDER BY created_at DESC LIMIT 10;"
```

**Zuko: browser sessions expired:**
- Run the session setup script: `python apps/zuko/setup_sessions.py`
- Follow the on-screen prompts to log in to SEEK and LinkedIn

**Docker: can't run commands from inside container:**
- Verify Docker socket is mounted: `-v /var/run/docker.sock:/var/run/docker.sock`
- Check `group_add` in `docker-compose.yml` matches the Docker GID on your host

---

## Viewing logs

```bash
# Local
tail -f logs/roderick.log

# Docker
docker compose logs -f roderick
docker compose logs -f zuko
docker compose logs -f api
```

---

## Inspecting the database

```bash
sqlite3 data/roderick.db

.tables
SELECT id, to_agent, status, description FROM tasks ORDER BY created_at DESC LIMIT 10;
SELECT name, status, last_message FROM agent_registry;
SELECT id, request_type, status FROM approval_requests ORDER BY created_at DESC LIMIT 10;
SELECT * FROM reminders WHERE done=0 ORDER BY due ASC;
SELECT agent_name, error_message, created_at FROM agent_errors ORDER BY created_at DESC LIMIT 10;
```

---

## Registering a new agent

1. Add to `config/agents.json`:
```json
{
  "name": "myagent",
  "display_name": "My Agent",
  "purpose": "What this agent does",
  "autonomy_level": "supervised",
  "task_types_accepted": ["my_task_type"],
  "status": "idle",
  "config": {}
}
```

2. Create `apps/myagent/agent.py` with a `run()` async polling loop.

3. Wire it in `apps/roderick/main.py`:
```python
my_agent = MyAgent(llm, db_path, config["data_dir"], config, owner_memory)
asyncio.create_task(my_agent.run())
```

4. Restart. The agent appears in `/agents`, the dashboard, and the watchdog automatically.
