# Deploying the rate limiter for free

Oracle Cloud Always Free VM + DuckDNS subdomain + Caddy for HTTPS. About 30–45 minutes the first time.

## Rules for staying at $0

- **Don't upgrade the account to Pay As You Go.** A Free Tier account can't be charged at all.
- Use only resources marked **Always Free eligible**: the `VM.Standard.A1.Flex` shape within the free allowance
  (currently 2 OCPUs and 12 GB of memory in total), and the default boot volume (the free allowance is 200 GB of
  block storage in total).
- Check Oracle's [Always Free Resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/resourceref.htm) page
  before you start; the limits have changed over time.

**Idle reclamation.** Oracle may reclaim an Always Free instance that is idle for 7 days (95th-percentile CPU under
20%, plus network and memory under 20% on A1). A portfolio demo is idle most of the time, so this can happen. Your
insurance: the demo GIF in the README, and this guide plus `scripts/deploy.sh`, which rebuild everything in about 15
minutes. Don't run fake load to dodge the policy.

## 1. Create the server

1. Sign up at cloud.oracle.com. Pick a home region near you; it can't be changed later.
2. **Compute → Instances → Create instance.**
   - Image: **Canonical Ubuntu 24.04** (the aarch64 build is chosen automatically for A1).
   - Shape: **Ampere → VM.Standard.A1.Flex**, 2 OCPUs, 12 GB memory.
   - Networking: let it create a new VCN and public subnet, and **assign a public IPv4 address**.
   - SSH keys: upload your public key (`~/.ssh/id_ed25519.pub`). Oracle images allow key login only.
   - If you get "out of host capacity", try another availability domain or try again later.
3. Note the instance's **public IP**.

## 2. Open ports 80 and 443

Two layers block traffic by default.

**Oracle security list:** Networking → Virtual cloud networks → your VCN → Security → the default security list → Add
ingress rules:

| Source CIDR | Protocol | Destination port |
|---|---|---|
| 0.0.0.0/0 | TCP | 80 |
| 0.0.0.0/0 | TCP | 443 |
| 0.0.0.0/0 | UDP | 443 (optional, HTTP/3) |

**The VM's own firewall:** Oracle's Ubuntu images ship iptables rules that reject everything except SSH.

```bash
ssh ubuntu@YOUR_IP
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p udp --dport 443 -j ACCEPT
sudo netfilter-persistent save
```

Don't open any other ports. Postgres, Redis, the API instances and the dashboard are never published to the internet.

## 3. Domain name (DuckDNS)

1. Sign in at duckdns.org, create a subdomain (e.g. `ratelimiter-yourname`), and set its IP to your server's public IP.
2. Your domain is `ratelimiter-yourname.duckdns.org`.

The IP of an Oracle instance stays the same while the instance exists, so this is a one-time step. If you recreate the
instance, update the IP on DuckDNS.

## 4. Install Docker

```bash
sudo apt-get update && sudo apt-get -y upgrade
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu
exit    # log out and back in so the group change applies
```

Ubuntu installs security updates automatically (`unattended-upgrades` is on by default). Check with
`systemctl status unattended-upgrades`.

## 5. Deploy

```bash
ssh ubuntu@YOUR_IP
git clone https://github.com/YOUR-USER/distributed-rate-limiter.git
cd distributed-rate-limiter
DOMAIN=ratelimiter-yourname.duckdns.org ./scripts/init-env.sh    # prints your admin password once
./scripts/deploy.sh
```

The first build on the ARM VM takes a few minutes. Caddy then gets a Let's Encrypt certificate, which needs ports 80
and 443 reachable and the DuckDNS record pointing at the server.

Check it:

```bash
curl -I https://ratelimiter-yourname.duckdns.org/            # 200 from the dashboard
docker compose --profile prod ps                            # everything "healthy" or "running"
docker compose logs caddy | tail                            # certificate messages if HTTPS fails
```

## 6. Reach the admin pages

Caddy doesn't expose `/admin` to the internet (it answers 404), so nobody can brute-force or lock the login. The
dashboard listens on `127.0.0.1:8000` on the server; reach it through an SSH tunnel from your laptop:

```bash
ssh -N -L 8000:127.0.0.1:8000 ubuntu@YOUR_IP
```

Leave that running and open http://localhost:8000/admin in your browser. Log in with the admin password from `.env`.

## 7. Make the showcase replay

1. Open the admin page through the tunnel.
2. Run demo 1 from the README (memory → redis halfway through).
3. Open the run, add a note, and **Pin this run**.
4. Open the live page in a private window: with no traffic, it replays the run.

Record a short GIF of the replay (e.g. with Peek, Kap or ScreenToGif), save it as `docs/demo.gif`, and commit it.

## Updating

```bash
cd distributed-rate-limiter && ./scripts/deploy.sh
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| Browser can't connect | Check both firewall layers in step 2, and that DuckDNS points at the right IP |
| Caddy logs ACME errors | Same as above: Let's Encrypt must reach port 80/443 on your domain |
| `api-*` unhealthy | `docker compose logs api-1`: usually a wrong password in `.env` after editing it by hand |
| Admin page says too many failed logins | Wait a minute; the lock is global by design |
| `https://your-domain/admin` returns 404 | Expected: use the SSH tunnel in step 6 |
| Out of memory | `docker stats`; the compose file sets memory limits per container |
| `/api/data` returns 502 after recreating containers | `docker compose restart nginx` (nginx resolves the API hostnames at startup) |
