# Deploying to DigitalOcean

Deploy steps for running this stack on a DigitalOcean Droplet. Recommended size: **2 vCPU / 4GB RAM** (~$24/mo) for comfortable headroom on Postgres+pgvector+FastAPI; the 2GB tier (~$12/mo) is a workable minimum if you're watching costs.

## 1. Create the droplet

1. Sign up at cloud.digitalocean.com, add a payment method.
2. **Create** → **Droplets**:
   - Region: nearest to your users
   - Image: **Ubuntu 24.04 LTS**
   - Size: Basic, Regular SSD, **4GB RAM / 2 vCPU**
   - Authentication: SSH key (add your local public key, e.g. `~/.ssh/id_ed25519.pub`) instead of a password
3. Create — note the public IP it assigns.

## 2. Lock down access (DigitalOcean Cloud Firewall)

**Networking → Firewalls** → create one, apply it to the droplet:
- Inbound: SSH (22) from your IP only, HTTP (80) + HTTPS (443) from anywhere
- Leave 8000 closed to the internet — it's reverse-proxied, not exposed directly

## 3. Initial server setup

```bash
ssh root@<droplet-ip>
apt update && apt upgrade -y
curl -fsSL https://get.docker.com | sh        # installs Docker + Compose plugin
apt install -y ufw
ufw allow 22 && ufw allow 80 && ufw allow 443 && ufw enable
```

## 4. Ship the app

Clone via git (rather than rsync) so later deploys — manual or via CI — can just `git pull`.

1. On GitHub: repo **Settings → Deploy keys → Add deploy key** (read-only is enough).
2. On the droplet, generate a key for that purpose and register the public half as the deploy key:

```bash
ssh root@<droplet-ip>
ssh-keygen -t ed25519 -f ~/.ssh/deploy_key -N ""
cat ~/.ssh/deploy_key.pub   # paste this into the GitHub deploy key field
```

3. Clone and configure:

```bash
GIT_SSH_COMMAND="ssh -i ~/.ssh/deploy_key" git clone git@github.com:iyerrajesh/security_rag.git /srv/security_rag
cd /srv/security_rag
git config core.sshCommand "ssh -i ~/.ssh/deploy_key"
cp .env.local .env
```

Edit `.env` on the server and fill in real `ANTHROPIC_API_KEY`, `VOYAGE_API_KEY`, a strong `POSTGRES_PASSWORD`, and a random `API_SECRET` (e.g. `openssl rand -hex 32`). Don't reuse a value you've shared anywhere else.

```bash
docker compose up -d --build
curl -H "X-Api-Key: $API_SECRET" localhost/health   # via Caddy on :80
```

`docker compose up` brings up `db`, `api`, and `caddy` together — `api` has no host port mapping (internal to the compose network only), and [Caddyfile](Caddyfile) reverse-proxies `:80` to `api:8000` *only* for requests carrying the correct `X-Api-Key` header (everything else gets `403`), so this one command gets you an access-gated HTTP endpoint with no extra TLS setup needed yet.

## 5. Add a domain + real TLS (later)

Right now [Caddyfile](Caddyfile) listens on bare `:80` since there's no domain — fine for testing, but unencrypted. Once you have a domain:

1. Point its A record at the droplet IP.
2. Edit [Caddyfile](Caddyfile), replacing `:80` with the domain:
   ```
   your-domain.com {
     reverse_proxy api:8000
   }
   ```
3. `docker compose up -d` — Caddy auto-issues and renews a Let's Encrypt cert, no other config needed.

## 6. Day-2 basics

- **Backups**: `docker compose exec db pg_dump -U raguser securityrag > backup.sql` on a cron, or enable DigitalOcean's Droplet Backups (~20% of droplet cost/mo) or manual Snapshots (one-off, billed for storage used).
- **Updates**: `git pull && docker compose up -d --build` on the droplet.
- **Logs**: `docker compose logs -f api`.
- **Monitoring**: enable DigitalOcean's free Droplet Monitoring agent for CPU/RAM/disk graphs and alerting.

## 7. CI deploy via GitHub Actions

[.github/workflows/deploy.yml](.github/workflows/deploy.yml) SSHes into the droplet on every push to `main` and re-pulls/rebuilds. It does **not** touch `.env` — that file lives only on the droplet (see step 4) and is never read from or written to git.

Add these repo secrets (**Settings → Secrets and variables → Actions**):

| Secret | Value |
|---|---|
| `DROPLET_HOST` | the droplet's public IP |
| `DROPLET_SSH_KEY` | private key whose public half is in the droplet's `~/.ssh/authorized_keys` (a dedicated CI key, not your personal one) |

Generate a CI-only key pair and add the public half to the droplet:
```bash
ssh-keygen -t ed25519 -f ci_deploy_key -N ""
ssh-copy-id -i ci_deploy_key.pub root@<droplet-ip>
# paste contents of ci_deploy_key (private) into the DROPLET_SSH_KEY secret
```

The workflow runs `docker compose up -d --build` and fails the run if `/health` doesn't return 200, so a bad deploy surfaces in the Actions tab instead of silently going live.
