# AWS Deployment Guide — Single EC2 (Dev/Demo)

Everything on one **t3.xlarge** (4 vCPU, 16 GB) via Docker Compose.

| Resource | Spec | Cost/Month |
|----------|------|------------|
| EC2 t3.xlarge | 4 vCPU, 16 GB | ~$120 on-demand / ~$75 reserved |
| EBS gp3 | 50 GB | ~$4 |
| Elastic IP | 1 | ~$3.65 (free while attached & instance running) |
| **Total** | | **~$124/mo on-demand / ~$79/mo reserved** |

---

## Prerequisites

1. **AWS Account** with permissions to create EC2, VPC, IAM resources
2. **AWS CLI** installed and configured (`aws configure`)
3. **EC2 Key Pair** already created in your target region
4. **GitHub repo** pushed (public, or use a PAT for private repos)

---

## Deploy in 2 Commands

### Option A: AWS Console (Click-Ops)

1. Go to **CloudFormation → Create Stack → Upload a template**
2. Upload `infra/cloudformation.yml`
3. Fill in the parameters:
   - `KeyPairName` — your existing key pair
   - `GitHubRepoUrl` — your GitHub clone URL
   - `AllowedSSHCidr` — your IP (e.g. `203.0.113.5/32`)
4. Click **Create Stack** and wait ~5 minutes

### Option B: AWS CLI (One-liner)

```bash
aws cloudformation deploy \
  --template-file infra/cloudformation.yml \
  --stack-name contract-guardian \
  --parameter-overrides \
      KeyPairName=my-key \
      GitHubRepoUrl=https://github.com/<YOUR_ORG>/Hackathon.git \
      GitBranch=main \
      AllowedSSHCidr=$(curl -s ifconfig.me)/32 \
  --capabilities CAPABILITY_IAM \
  --region us-east-1
```

### Get Your URLs

```bash
aws cloudformation describe-stacks \
  --stack-name contract-guardian \
  --query "Stacks[0].Outputs" \
  --output table
```

---

## What Happens Automatically

The EC2 user-data script runs on first boot and:

1. Installs Docker + Docker Compose v2
2. Tunes `vm.max_map_count=262144` for Elasticsearch
3. Clones your GitHub repo
4. Creates `.env` from `.env.example` with working defaults
5. Runs `docker compose up -d --build`

The stack takes **3-5 minutes** to fully boot after the instance is running.

---

## Access Your Services

| Service | URL | Credentials |
|---------|-----|-------------|
| OpenMetadata UI | `http://<EIP>:8585` | admin / admin |
| Airflow UI | `http://<EIP>:8080` | admin / admin |
| Dashboard | `http://<EIP>:8501` | — |
| MCP Server | `http://<EIP>:8000/mcp` | — |

---

## SSH Into the Instance

```bash
ssh -i my-key.pem ec2-user@<EIP>

# Check bootstrap progress
tail -f /var/log/user-data.log

# Check containers
cd ~/app
docker compose ps
docker compose logs -f --tail=50
```

---

## Seed Data (After Stack is Healthy)

```bash
ssh -i my-key.pem ec2-user@<EIP>
cd ~/app

# Wait for OpenMetadata API
until curl -sf http://localhost:8585/api/v1/system/version; do sleep 10; done

# Run seed
docker compose exec contract-sync python /app/sync.py
```

---

## Tear Down

```bash
aws cloudformation delete-stack --stack-name contract-guardian --region us-east-1
```

This removes the EC2 instance, EBS volume, VPC, security group, and Elastic IP. Complete cleanup.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Containers OOM-killed | Check `docker stats` — instance may need t3.2xlarge |
| Elasticsearch won't start | Verify `vm.max_map_count`: `sysctl vm.max_map_count` |
| Can't reach UI | Check security group allows your IP on the port |
| User-data failed | `cat /var/log/user-data.log` for the full bootstrap log |
| Private repo clone fails | Use `https://<PAT>@github.com/...` or set up deploy keys |

---

## Cost Optimization Tips

- **Stop when not in use**: `aws ec2 stop-instances --instance-ids <id>` — EBS costs ~$4/mo even when stopped, but EC2 charges stop
- **Reserved Instance**: Commit 1-year for ~37% savings ($75/mo vs $120/mo)
- **Spot Instance**: For non-critical demos, spot t3.xlarge is ~$36/mo (70% off) but can be interrupted
