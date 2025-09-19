# Exchange EWS Connector

A scalable Exchange Online connector that synchronizes emails to Amazon Q Business using AWS Copilot for deployment. Supports splitting email accounts across multiple containers for better parallelization.

## Prerequisites

1. **AWS CLI** configured with appropriate permissions
2. **AWS Copilot CLI** installed
3. **Docker** installed and running
4. **Exchange Online** application registered in Azure AD
5. **Amazon Q Business** application, index, and data source created

## Architecture Options

### Single Container Mode
- One service processes all email accounts continuously
- Simpler setup and monitoring
- Suitable for smaller deployments (< 10 email accounts)

### Split Container Mode
- Multiple services process different email accounts
- Better parallelization and resource utilization
- Suitable for larger deployments (10+ email accounts)
- Each container processes a subset of accounts using round-robin assignment

## Infrastructure Design

### DynamoDB Table Management
- **Dynamic Creation**: The application creates the DynamoDB table automatically on first run
- **Multi-Container Safe**: Handles race conditions when multiple containers start simultaneously
- **Correct Schema**: Ensures the table schema matches exactly what the application expects
- **No CloudFormation Dependency**: Eliminates potential schema mismatches between infrastructure and application code

### Internal Sync Management
- **Thread-Based Execution**: Sync runs in separate thread to avoid blocking main process
- **Overlap Prevention**: Built-in check prevents multiple syncs from running simultaneously
- **Container Isolation**: Each container manages its own sync schedule independently
- **Graceful Shutdown**: Waits for current sync to complete before stopping

### Benefits of Continuous Running:
1. **Always Available**: Container stays running, reducing startup overhead
2. **Health Monitoring**: Built-in health check endpoints for monitoring
3. **Resource Efficiency**: No container startup/shutdown overhead every 24 hours
4. **Better Logging**: Continuous logging stream instead of separate job logs

### Benefits of Dynamic Table Creation:
1. **Schema Consistency**: Application code defines the exact schema it needs
2. **Race Condition Handling**: Built-in logic for multiple containers creating the same table
3. **Simplified Deployment**: No need to manage table creation separately
4. **Environment Flexibility**: Works across different environments without configuration changes

## Account Splitting Strategy

The system uses **round-robin assignment** to distribute email accounts across containers:

### Example with 7 accounts and 3 containers:
- **Container 1** (index 0): accounts[0], accounts[3], accounts[6] → `user1@domain.com`, `user4@domain.com`, `user7@domain.com`
- **Container 2** (index 1): accounts[1], accounts[4] → `user2@domain.com`, `user5@domain.com`
- **Container 3** (index 2): accounts[2], accounts[5] → `user3@domain.com`, `user6@domain.com`

### Configuration Options:

1. **Environment Variables**:
   ```bash
   CONTAINER_INDEX=0        # 0-based index (0, 1, 2, ...)
   TOTAL_CONTAINERS=3       # Total number of containers
   ```

2. **Parameter Store**:
   ```
   EXCHANGE_PRIMARY_SMTP_ADDRESS=user1@domain.com,user2@domain.com,user3@domain.com,user4@domain.com,user5@domain.com,user6@domain.com,user7@domain.com
   ```

## Quick Start

### 1. Install AWS Copilot CLI

```bash
# Linux/macOS
curl -Lo copilot https://github.com/aws/copilot-cli/releases/latest/download/copilot-linux
chmod +x copilot && sudo mv copilot /usr/local/bin

# Or using Homebrew (macOS)
brew install aws/tap/copilot-cli
```

### 2. Setup Parameters

Create the required parameters in AWS Parameter Store:

```bash
# Interactive setup
./setup-parameters.sh

# Or with config file
cp config.sample.json config.json
# Edit config.json with your values
./setup-parameters.sh --config-file config.json
```

### 3. Deploy

#### Single Container Mode:
```bash
./deploy.sh
# OR
./deploy.sh --mode single
```

#### Split Container Mode:
```bash
# Generate 3 workers
./generate-workers.sh 3

# Deploy the workers
./deploy.sh --mode split
```

## Configuration Files

### Project Structure:
```
exchange-ews-connector/
├── copilot/
│   ├── environments/
│   │   └── addons/
│   │       └── infrastructure.yml       # Minimal infrastructure (no DynamoDB)
│   └── exchange-sync/                   # Main service configuration
│       ├── copilot.yml                  # Used for both single and split modes
│       └── addons/
│           └── iam-policy.yml           # IAM permissions including DynamoDB
├── modules/                             # Python modules
├── health_server.py                    # Built-in health check server
├── Dockerfile                          # Uses AWS ECR public images
├── config.sample.json                  # Configuration template
├── deploy.sh                            # Deployment script
├── setup-parameters.sh                 # Parameter Store setup
└── generate-workers.sh                 # Creates multiple workers from main config
```

### Environment Variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `CONTAINER_INDEX` | Container index (0-based) | `0`, `1`, `2` |
| `TOTAL_CONTAINERS` | Total number of containers | `3` |
| `SYNC_MODE` | Sync mode | `delta`, `full` |
| `ENABLE_THREADING` | Enable parallel processing | `true` |
| `MAX_WORKER_THREADS` | Worker threads per container | `4` |
| `DOCUMENT_BATCH_SIZE` | Q Business batch size | `10` |

## Scheduling and Process Management

### Continuous Running Containers
The application runs as a Backend Service with internal 24-hour scheduling:
```yaml
type: Backend Service
count: 1  # Single instance per environment
```

### Internal Scheduling Behavior
- **Container Lifecycle**: Always running (not scheduled tasks)
- **Sync Interval**: 24 hours between sync operations
- **Overlap Prevention**: Built-in check to prevent concurrent syncs within same container
- **Graceful Shutdown**: Handles SIGTERM/SIGINT signals properly

### Example Process Flow
```
Container Start: Health server starts → Initial sync runs → Wait 24 hours → Next sync → Repeat
If sync takes > 24 hours: Next cycle waits for current sync to complete
Container Stop: Graceful shutdown → Wait for current sync → Stop health server → Exit
```

### Health Check Endpoints
The application includes a built-in HTTP server for health monitoring:
```
GET /health  - Simple health check (returns 200 if healthy)
GET /status  - Detailed status including uptime and configuration
```

**Port**: 8080 (internal container port)
**Access**: Available through Copilot service endpoint

## Usage

### Manual Execution (for testing):
```bash
# Run continuously (default)
python qbusiness_ews_sync.py

# Run once and exit (for testing)
python qbusiness_ews_sync.py --once

# Run with specific sync mode
python qbusiness_ews_sync.py delta
python qbusiness_ews_sync.py full
```

### Container Deployment:
- **Single container**: Processes all accounts continuously
- **Split containers**: Each processes a subset of accounts continuously
- **Health monitoring**: Built-in endpoints at `/health` and `/status`

## Monitoring and Troubleshooting

### View Logs:
```bash
# Single container
copilot svc logs --name exchange-sync --env dev

# Split containers (example with 3 workers)
copilot svc logs --name exchange-sync-worker-1 --env dev
copilot svc logs --name exchange-sync-worker-2 --env dev
copilot svc logs --name exchange-sync-worker-3 --env dev
```

### Check Service Status:
```bash
copilot svc ls
copilot svc status --name exchange-sync --env dev
```

### Health Checks:
```bash
# Get service endpoint
copilot svc show --name exchange-sync --env dev

# Check health (replace with actual endpoint)
curl https://your-service-endpoint/health
curl https://your-service-endpoint/status
```

### CloudWatch Metrics:
- Monitor ECS task execution
- Check DynamoDB read/write metrics
- Monitor Q Business API calls

### Common Issues:

1. **Parameter Store Access**: Ensure IAM roles have SSM permissions
2. **Q Business Limits**: Batch size should be ≤ 10 documents
3. **Container Splitting**: Verify `CONTAINER_INDEX` and `TOTAL_CONTAINERS` are set correctly
4. **Account Assignment**: Check logs to see which accounts are assigned to each container

## Scaling Recommendations

### Small Deployment (1-5 accounts):
- Use single container mode
- 1 vCPU, 2 GB RAM
- Sync every 24 hours

### Medium Deployment (6-20 accounts):
- Use 3 split containers
- 1 vCPU, 2 GB RAM per container
- 4 worker threads per container
- Sync every 24 hours

### Large Deployment (20+ accounts):
- Use 5+ split containers
- 1 vCPU, 2 GB RAM per container
- 4 worker threads per container
- Sync every 24 hours (containers run continuously)
- Monitor Q Business API limits

## Advanced Configuration

### Manual Parameter Creation:
If you prefer to create parameters manually, create these in AWS Parameter Store:

```bash
# Required parameters (replace values with your actual configuration)
# Using existing parameter paths for backwards compatibility
/exchange-connector/dev/exchange-client-id                  # SecureString
/exchange-connector/dev/exchange-client-secret              # SecureString  
/exchange-connector/dev/exchange-tenant-id                  # SecureString
/exchange-connector/dev/exchange-primary-smtp-address       # SecureString
/exchange-connector/dev/qbusiness-application-id            # String
/exchange-connector/dev/qbusiness-index-id                  # String
/exchange-connector/dev/qbusiness-datasource-id             # String
```

### Manual Deployment Steps:

```bash
# IMPORTANT: Setup parameters first
./setup-parameters.sh --config-file config.json

# Initialize application
copilot app init exchange-connector

# Initialize and deploy environment
copilot env init --name dev
copilot env deploy --name dev

# For single container:
copilot svc init exchange-sync
copilot svc deploy --name exchange-sync --env dev

# For split containers (generate workers first):
./generate-workers.sh 3  # Creates 3 worker configurations
./deploy.sh --mode split --env dev
```

### Custom Configuration:
```bash
./deploy.sh --app my-exchange-app --env production --mode split
```

## Security Best Practices

1. **Use Parameter Store SecureString** for sensitive values
2. **Implement least-privilege IAM policies**
3. **Enable VPC Flow Logs** for network monitoring
4. **Use AWS CloudTrail** for API call auditing
5. **Regularly rotate Exchange credentials**

## Cost Optimization

1. **Right-size containers**: Start with smaller resources and scale up
2. **Adjust schedule**: Less frequent runs for stable environments
3. **Use Spot instances**: For non-critical workloads
4. **Monitor unused resources**: Remove containers with no assigned accounts

## Cleanup

To remove the deployment:

```bash
# Delete services
copilot svc delete --name exchange-sync --env dev

# Or for split mode (delete all generated workers)
copilot svc delete --name exchange-sync-worker-1 --env dev
copilot svc delete --name exchange-sync-worker-2 --env dev
copilot svc delete --name exchange-sync-worker-3 --env dev
# (adjust based on number of workers you created)

# Delete environment
copilot env delete --name dev

# Delete application
copilot app delete exchange-connector
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.