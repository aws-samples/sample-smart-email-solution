# Exchange Online Archive Connector for AWS EC2

This project extends the Exchange Online Archive connector to run on AWS EC2 instances with proper IAM role configuration for secure access to AWS services.

## Architecture

- **AWS EC2**: Runs the Exchange connector code as a scheduled service
- **Amazon DynamoDB**: Tracks processed emails to avoid duplicates (automatically created if not exists)
- **Amazon Q Business**: Indexes email content for search and retrieval using official Exchange field mappings (supports both HTML and plain text formats)
- **AWS Systems Manager Parameter Store**: Securely stores configuration parameters
- **CloudWatch**: Monitors execution and logs
- **IAM Role**: Provides secure access to AWS services without hardcoded credentials

### Deployment Architecture

**What's Deployed on EC2:**
- Python application with `qbusiness_ews_sync.py` code
- Systemd service for automated execution
- Cron job for scheduled runs

**What Must Be Created:**
- EC2 instance with appropriate IAM role
- DynamoDB table for email tracking (optional - will be auto-created if not exists)
- CloudWatch log groups for monitoring
- Q Business application, index, and data source

## Prerequisites

### Microsoft 365 / Exchange Online Setup

1. **Microsoft Exchange Account**: Create a Microsoft Exchange account in Office 365
2. **Tenant ID**: Copy your Microsoft 365 tenant ID from the Properties section of your Azure Active Directory Portal
   - Find your tenant ID: [Microsoft 365 tenant ID documentation](https://docs.microsoft.com/en-us/onedrive/find-your-office-365-tenant-id)
3. **OAuth 2.0 Application**: Configure an OAuth 2.0 credential token with client ID and client secret

### Required Application Permissions

Configure the following permissions for your connector application in Azure AD:

#### Office 365 Exchange Online Permissions:
- `EWS.AccessAsUser.All` (Delegated)
- `full_access_as_app` (Application)

### AWS Prerequisites

1. **Amazon Q Business** application, index, and data source configured
2. **AWS CLI** configured with appropriate permissions
3. **Python 3.11** for local development/testing

## Required AWS Permissions

### For EC2 Instance Setup
The deployment requires the following AWS permissions:
- **EC2**: Launch and manage instances
- **IAM**: Create roles and policies for EC2 instance profile
- **Systems Manager**: Create and manage Parameter Store parameters

### For EC2 Instance Runtime
The EC2 instance requires an IAM role with the following permissions:
- **DynamoDB**: Read/write access to the specified table
- **Systems Manager**: Read access to Parameter Store parameters
- **Q Business**: Access to your application, index, and data source
- **CloudWatch**: Write access for logs

### Additional Setup Required
You must separately create:
- **DynamoDB Table**: For email tracking (see DynamoDB Schema section)
- **CloudWatch Log Groups**: For monitoring (if needed)
- **Q Business Resources**: Application, index, and data source

## Files Overview

- `qbusiness_ews_sync.py`: **Exchange connector application** - includes all functionality (EWS client, document processor, Q Business client, DynamoDB client, and configuration)
- `requirements.txt`: Python dependencies
- `deploy-config.sh`: **Configuration deployment script** - creates Parameter Store parameters from config file
- `config.sample.json`: Template for deployment configuration
- `config.json`: Your deployment configuration (create from config.sample.json, not committed to git)
- `README.md`: This documentation

### Architecture Changes

**EC2 Deployment Structure:**
- **Single Python Application**: All functionality in `qbusiness_ews_sync.py` for straightforward EC2 deployment
- **IAM Role-Based Security**: Uses EC2 instance profile for secure AWS service access
- **Systemd Service**: Runs as a managed service with automatic restarts
- **Cron Scheduling**: Uses standard cron

## Configuration

Configuration is stored securely in **AWS Systems Manager Parameter Store**. The deployment scripts automatically create these parameters from your `config.json` file.

### Azure AD Application Setup

#### Step 1: Register Application
1. Go to [Azure Portal](https://portal.azure.com) → Azure Active Directory → App registrations
2. Click "New registration"
3. Provide a name (e.g., "Exchange Online Archive Connector")
4. Select "Accounts in this organizational directory only"
5. Click "Register"

#### Step 2: Configure API Permissions
Add the following API permissions:

**Office 365 Exchange Online:**
1. Go to "API permissions" → "Add a permission" → "APIs my organization uses"
2. Search for "Office 365 Exchange Online"
3. Select "Delegated permissions" and add:
   - `EWS.AccessAsUser.All`
4. Select "Application permissions" and add:
   - `full_access_as_app`

#### Step 3: Grant Admin Consent
1. Click "Grant admin consent for [Your Organization]"
2. Confirm the consent

#### Step 4: Generate Client Secret
1. Go to "Certificates & secrets" → "Client secrets"
2. Click "New client secret"
3. Add description and set expiration
4. Copy the secret value (you won't see it again)

#### Step 5: Collect Required Information
Update your `config.json` file with:
- `exchange.client_id`: Application (client) ID from the Overview page
- `exchange.client_secret`: Client secret value from Step 4
- `exchange.tenant_id`: Directory (tenant) ID from the Overview page
- `exchange.primary_smtp_address`: Target mailbox email address(es)
  - Single address: `user@company.com`
  - Multiple addresses: `user1@company.com,user2@company.com,user3@company.com`

### Amazon Q Business Setup

1. Create a Q Business application
2. Create an index within the application
3. Create a data source within the index
4. Update your `config.json` file with:
   - `qbusiness.application_id`: Application ID
   - `qbusiness.index_id`: Index ID
   - `qbusiness.datasource_id`: Data Source ID

### Multi-Account Processing

The connector supports processing multiple Exchange mailboxes in a single execution. This is useful for organizations that need to index emails from multiple users or shared mailboxes.

#### Configuration Examples

**Single mailbox:**
```json
{
  "exchange": {
    "primary_smtp_address": "user@company.com"
  }
}
```

**Multiple mailboxes:**
```json
{
  "exchange": {
    "primary_smtp_address": "user1@company.com,user2@company.com,user3@company.com"
  }
}
```

#### Processing Behavior

- Each mailbox is processed sequentially
- The `EMAIL_PROCESSING_LIMIT` applies across all mailboxes (not per mailbox)
- If one mailbox fails to connect, processing continues with the remaining mailboxes
- Each mailbox connection is authenticated independently
- Archive folders are always processed for each account (if available)
- Main mailbox folders are processed only if `PROCESS_MAIN_MAILBOX=true` is set

#### Requirements for Multi-Account Processing

1. **Azure AD Application Permissions**: Your Azure AD application must have the necessary permissions to access all target mailboxes
2. **Impersonation Rights**: The application must have impersonation rights for each mailbox
3. **Valid Email Addresses**: All email addresses must be valid and accessible within your tenant

### Parameter Store Structure

When deployed to AWS, the configuration is stored in Parameter Store with this structure:
```
/exchange-connector/prod/
├── exchange-client-id (SecureString)
├── exchange-client-secret (SecureString)
├── exchange-tenant-id (SecureString)
├── exchange-primary-smtp-address (String)
├── exchange-server (String)
├── qbusiness-application-id (String)
├── qbusiness-index-id (String)
└── qbusiness-data-source-id (String)
```

## EC2 Deployment

### Prerequisites
- **AWS CLI** configured with appropriate permissions
- **Python 3.11+** installed on EC2 instance
- **jq** for JSON processing (config deployment script)

### Step 1: Create IAM Role for EC2

Create an IAM role that your EC2 instance will use to access AWS services:

#### Create IAM Role Policy Document

Create a file named `ec2-trust-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

#### Create IAM Policy for Exchange Connector

Create a file named `exchange-connector-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DescribeTable",
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:BatchGetItem",
        "dynamodb:BatchWriteItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/processed-emails",
        "arn:aws:dynamodb:*:*:table/processed-emails/index/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParametersByPath"
      ],
      "Resource": "arn:aws:ssm:*:*:parameter/exchange-connector/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "qbusiness:BatchPutDocument",
        "qbusiness:BatchDeleteDocument",
        "qbusiness:StartDataSourceSyncJob",
        "qbusiness:StopDataSourceSyncJob",
        "qbusiness:GetDataSourceSyncJob",
        "qbusiness:ListDataSourceSyncJobs"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/ec2/exchange-connector*"
    }
  ]
}
```

#### Create the IAM Role and Instance Profile

```bash
# Create the IAM role
aws iam create-role \
    --role-name ExchangeConnectorRole \
    --assume-role-policy-document file://ec2-trust-policy.json

# Create the IAM policy
aws iam create-policy \
    --policy-name ExchangeConnectorPolicy \
    --policy-document file://exchange-connector-policy.json

# Attach the policy to the role
aws iam attach-role-policy \
    --role-name ExchangeConnectorRole \
    --policy-arn arn:aws:iam::YOUR_ACCOUNT_ID:policy/ExchangeConnectorPolicy

# Create instance profile
aws iam create-instance-profile \
    --instance-profile-name ExchangeConnectorInstanceProfile

# Add role to instance profile
aws iam add-role-to-instance-profile \
    --instance-profile-name ExchangeConnectorInstanceProfile \
    --role-name ExchangeConnectorRole
```

### Step 2: Launch EC2 Instance

Launch an EC2 instance with the IAM role:

```bash
aws ec2 run-instances \
    --image-id ami-0abcdef1234567890 \
    --count 1 \
    --instance-type t3.medium \
    --key-name your-key-pair \
    --security-group-ids sg-12345678 \
    --subnet-id subnet-12345678 \
    --iam-instance-profile Name=ExchangeConnectorInstanceProfile \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=ExchangeConnector}]'
```

### Step 3: Configure Your Settings

Create your configuration file:
```bash
cp config.sample.json config.json
# Edit config.json with your Exchange and Q Business settings
```

### Step 4: Deploy Configuration to Parameter Store

```bash
./deploy-config.sh config.json
```

This script will:
- Validate your configuration file
- Create secure parameters in AWS Systems Manager Parameter Store
- Set up proper parameter encryption and access controls

### Step 5: Set Up EC2 Instance

SSH into your EC2 instance and set up the application:

```bash
# Connect to your EC2 instance
ssh -i your-key.pem ec2-user@your-instance-ip

# Update system packages
sudo yum update -y

# Install Python 3.11 and pip
sudo yum install -y python3.11 python3.11-pip git

# Clone your repository (or upload files)
git clone your-repository-url
cd exchange-ews-connector

# Install Python dependencies
pip3.11 install -r requirements.txt

# Set environment variables
export PARAMETER_PREFIX="/exchange-connector/prod"
export DYNAMODB_TABLE_NAME="processed-emails"
export EMAIL_PROCESSING_LIMIT="50"
export AWS_DEFAULT_REGION="us-east-1"

# Test the application
python3.11 qbusiness_ews_sync.py
```

### Step 6: Create Systemd Service

Create a systemd service for automatic management:

```bash
# Create service file
sudo tee /etc/systemd/system/exchange-connector.service > /dev/null <<EOF
[Unit]
Description=Exchange Online Archive Connector
After=network.target

[Service]
Type=oneshot
User=ec2-user
WorkingDirectory=/home/ec2-user/exchange-ews-connector
Environment=PARAMETER_PREFIX=/exchange-connector/prod
Environment=DYNAMODB_TABLE_NAME=processed-emails
Environment=EMAIL_PROCESSING_LIMIT=50
Environment=AWS_DEFAULT_REGION=us-east-1
ExecStart=/usr/bin/python3.11 qbusiness_ews_sync.py
StandardOutput=journal
StandardError=journal
SyslogIdentifier=exchange-connector

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable service
sudo systemctl daemon-reload
sudo systemctl enable exchange-connector.service

# Test the service
sudo systemctl start exchange-connector.service
sudo systemctl status exchange-connector.service
```

### Step 7: Set Up Cron Job for Scheduling

Create a cron job to run every day once:

```bash
# Edit crontab
crontab -e

# Add this line to run daily at 12pm
0 12 * * * /usr/bin/systemctl start exchange-connector.service

# Verify cron job
crontab -l
```

### Alternative: Using Systemd Timer

Instead of cron, you can use systemd timers:

```bash
# Create timer file
sudo tee /etc/systemd/system/exchange-connector.timer > /dev/null <<EOF
[Unit]
Description=Run Exchange Connector daily at 12pm
Requires=exchange-connector.service

[Timer]
OnCalendar=*-*-* 12:00:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

# Enable and start timer
sudo systemctl daemon-reload
sudo systemctl enable exchange-connector.timer
sudo systemctl start exchange-connector.timer

# Check timer status
sudo systemctl status exchange-connector.timer
sudo systemctl list-timers exchange-connector.timer
```

## Configuration Parameters

### AWS Deployment (Parameter Store)

For AWS deployment, sensitive configuration is stored in Parameter Store:

| Parameter Store Path | Description | Type |
|---------------------|-------------|------|
| `/exchange-connector/prod/exchange-client-id` | Azure AD Application Client ID | SecureString |
| `/exchange-connector/prod/exchange-client-secret` | Azure AD Application Client Secret | SecureString |
| `/exchange-connector/prod/exchange-tenant-id` | Azure AD Tenant ID | SecureString |
| `/exchange-connector/prod/exchange-primary-smtp-address` | Exchange mailbox email address | String |
| `/exchange-connector/prod/exchange-server` | Exchange server URL | String |
| `/exchange-connector/prod/qbusiness-application-id` | Amazon Q Business Application ID | String |
| `/exchange-connector/prod/qbusiness-index-id` | Amazon Q Business Index ID | String |
| `/exchange-connector/prod/qbusiness-data-source-id` | Amazon Q Business Data Source ID | String |

### Environment Variables for EC2

Set these environment variables in your systemd service or shell profile:

| Variable | Description | Default |
|----------|-------------|---------|
| PARAMETER_PREFIX | Prefix for Parameter Store parameters | /exchange-connector/prod |
| EMAIL_PROCESSING_LIMIT | Max emails per execution | 50 |
| AWS_DEFAULT_REGION | AWS region for services | us-east-1 |
| DYNAMODB_TABLE_NAME | Name of DynamoDB table | processed-emails |
| LOG_LEVEL | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |

**Note**: The EC2 instance uses IAM roles for authentication, eliminating the need for hardcoded AWS credentials.

## DynamoDB Table Setup

**New Feature**: The DynamoDB table is now automatically created if it doesn't exist. However, you can still create it manually if preferred.

### Automatic Table Creation

The connector will automatically create the DynamoDB table with the following features:
- **Table Name**: Uses the `DYNAMODB_TABLE_NAME` environment variable (default: `processed-emails`)
- **Primary Key**: `email_id` (String)
- **Global Secondary Index**: `account-email-index` for efficient querying by account
- **Billing Mode**: Pay-per-request (on-demand)
- **Tags**: Automatically tagged with Application, Environment, and CreatedBy

### Manual Table Creation (Optional)

#### Option 1: AWS CLI
```bash
aws dynamodb create-table \
    --table-name processed-emails \
    --attribute-definitions \
        AttributeName=email_id,AttributeType=S \
        AttributeName=account_email,AttributeType=S \
    --key-schema \
        AttributeName=email_id,KeyType=HASH \
    --global-secondary-indexes \
        IndexName=account-email-index,KeySchema=[{AttributeName=account_email,KeyType=HASH}],Projection={ProjectionType=ALL},BillingMode=PAY_PER_REQUEST \
    --billing-mode PAY_PER_REQUEST \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
```

#### Option 2: AWS Console
1. Go to DynamoDB in AWS Console
2. Click "Create table"
3. Set table name: `processed-emails`
4. Set partition key: `email_id` (String)
5. Click "Create global secondary index"
   - Index name: `account-email-index`
   - Partition key: `account_email` (String)
6. Choose "On-demand" billing mode
7. Enable point-in-time recovery
8. Create the table

### DynamoDB Schema

The connector uses a DynamoDB table to track processed emails and prevent duplicates:

#### Primary Table Structure
| Field | Type | Description |
|-------|------|-------------|
| `email_id` | String (Hash Key) | Unique identifier for each email |
| `account_email` | String | Email address of the account this email belongs to |
| `folder_name` | String | Exchange folder name where the email was found |
| `datetime_created` | String | Email creation timestamp |
| `processed_at` | String | When the email was processed by the connector |
| `status` | String | Processing status (`processed` or `failed`) |
| `attempt_count` | Number | Number of processing attempts |

#### Global Secondary Index
- **Index Name**: `account-email-index`
- **Hash Key**: `account_email`
- **Projection**: All attributes

This index enables efficient querying of emails by account, useful for:
- Account-specific cleanup operations
- Processing statistics per account
- Troubleshooting specific mailbox issues

## Scheduling Options

The connector can be scheduled using either cron or systemd timers:

### Systemd Timer

```bash
# Create timer file
sudo tee /etc/systemd/system/exchange-connector.timer > /dev/null <<EOF
[Unit]
Description=Run Exchange Connector daily at 12pm
Requires=exchange-connector.service

[Timer]
OnCalendar=*-*-* 12:00:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

# Enable and start timer
sudo systemctl enable exchange-connector.timer
sudo systemctl start exchange-connector.timer
```

### Manual Execution

Run the connector manually for testing:

```bash
# Using systemctl
sudo systemctl start exchange-connector.service

# Direct execution
cd /home/ec2-user/exchange-ews-connector
python3.11 qbusiness_ews_sync.py
```

## Monitoring

### System Logs

Monitor application execution using journalctl:
```bash
# View recent logs
sudo journalctl -u exchange-connector.service -f

# View logs from last hour
sudo journalctl -u exchange-connector.service --since "1 hour ago"

# View logs with specific priority
sudo journalctl -u exchange-connector.service -p err
```

### CloudWatch Logs (Optional)

Set up CloudWatch agent for centralized logging:

```bash
# Install CloudWatch agent
sudo yum install -y amazon-cloudwatch-agent

# Configure CloudWatch agent
sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json > /dev/null <<EOF
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/messages",
            "log_group_name": "/aws/ec2/exchange-connector/system",
            "log_stream_name": "{instance_id}"
          }
        ]
      },
      "journal": {
        "log_group_name": "/aws/ec2/exchange-connector/application",
        "log_stream_name": "{instance_id}",
        "unit_whitelist": ["exchange-connector.service"]
      }
    }
  }
}
EOF

# Start CloudWatch agent
sudo systemctl enable amazon-cloudwatch-agent
sudo systemctl start amazon-cloudwatch-agent
```

### Service Status Monitoring

Check service status and health:
```bash
# Service status
sudo systemctl status exchange-connector.service

# Timer status (if using systemd timer)
sudo systemctl status exchange-connector.timer

# View recent executions
sudo systemctl list-timers exchange-connector.timer

# Check cron jobs
crontab -l
```

## Troubleshooting

### Common Issues

1. **Microsoft 365 Authentication Errors**:
   - **Permission Issues**: Ensure all required permissions are granted and admin consent is provided
   - **Tenant ID**: Verify the tenant ID is correct (found in Azure AD Properties)
   - **Client Secret**: Check if the client secret has expired and generate a new one if needed
   - **Application ID**: Confirm the client ID matches the registered application
   - **Mailbox Access**: Ensure the application has access to the target mailbox

2. **Exchange Online Connectivity**:
   - **EWS Access**: Verify EWS (Exchange Web Services) is enabled for your organization
   - **Mailbox Permissions**: Confirm the application has full access to the target mailbox
   - **Network Connectivity**: Check if you can reach Exchange Online endpoints
   - **Throttling**: Exchange Online may throttle requests; the connector handles this automatically

3. **Q Business Errors**:
   - Verify application, index, and data source IDs
   - Check IAM permissions for Q Business access
   - Ensure the data source is properly configured

4. **Sync Job Conflicts**:
   - The connector automatically handles ConflictException when starting sync jobs
   - If another sync job is running, it will be stopped automatically (configurable)
   - Configure `AUTO_RESOLVE_SYNC_CONFLICTS=false` to disable automatic conflict resolution
   - Adjust `MAX_SYNC_CONFLICT_RETRIES` to control retry attempts (default: 3)

5. **EC2 Execution**:
   - Check systemd service status: `sudo systemctl status exchange-connector.service`
   - View application logs: `sudo journalctl -u exchange-connector.service -f`
   - Verify Parameter Store parameters are correctly set
   - Ensure IAM role has proper permissions

6. **DynamoDB Errors**:
   - Verify IAM permissions for DynamoDB
   - Check table creation and access

### Debug Mode

To enable verbose logging, set the environment variable in your systemd service:
- Add `Environment=LOG_LEVEL=DEBUG` to the service file
- Or export it in your shell: `export LOG_LEVEL=DEBUG`

### Performance Tuning

- **Instance Type**: Use larger EC2 instances for better performance (t3.medium or larger recommended)
- **Processing Limit**: Reduce for faster execution, increase for more throughput
- **Batch Size**: Modify in code for optimal Q Business indexing
- **Scheduling**: Adjust cron frequency based on email volume

#### HTML Processing Optimizations

The connector includes optimized HTML-to-text conversion for large emails:

- **HTML_PROCESSING_THRESHOLD**: Switch to optimized processing for emails larger than this size (default: 100,000 chars)
- **HTML_CHUNK_SIZE**: Process extremely large HTML emails in chunks (default: 500,000 chars)
- **MAX_CONTENT_SIZE_MB**: Pre-truncate emails larger than this before processing (default: 10 MB)

These settings help prevent timeouts when processing large HTML emails with complex formatting. The system automatically:
- Uses standard processing for small emails
- Switches to optimized regex patterns for medium emails
- Uses chunked processing for very large emails
- Falls back to simple tag stripping if conversion fails

#### Sync Job Conflict Resolution

The connector includes automatic handling of Q Business sync job conflicts:

- **AUTO_RESOLVE_SYNC_CONFLICTS**: Automatically stop existing sync jobs when conflicts occur (default: true)
- **MAX_SYNC_CONFLICT_RETRIES**: Number of retry attempts for conflict resolution (default: 3)

This prevents failures when multiple executions try to start sync jobs simultaneously.

#### Memory-Efficient Document Processing

The connector uses streaming document processing to minimize memory usage:

- **DOCUMENT_BATCH_SIZE**: Number of documents to process before submitting to Q Business (default: 10)

Instead of collecting all documents in memory, the system:
- Processes emails in small batches
- Submits documents to Q Business immediately when batch size is reached
- Marks emails as processed in DynamoDB after successful submission
- Reduces memory footprint for large mailboxes

#### Consistent Data Management

The connector ensures data consistency between Q Business and DynamoDB:

- **Synchronized Deletions**: When emails are deleted from DynamoDB tracking, corresponding documents are automatically deleted from Q Business
- **Orphaned Item Cleanup**: Identifies and removes documents that exist in Q Business but no longer exist in Exchange
- **Full Sync Preparation**: Clears both Q Business documents and DynamoDB records before reprocessing all emails
- **Atomic Operations**: Document and record deletions are performed together to maintain consistency

## Security Considerations

### Exchange Permissions and Data Safety

⚠️ **Important Security Notice**: The current implementation uses `EWS.AccessAsUser.All` delegated permission, which provides **read AND write access** to Exchange data. While the connector code only performs read operations, the permission itself allows data modification.

#### Current Permission Capabilities:
- ✅ **Read emails, calendars, contacts** (used by connector)
- ⚠️ **Modify/delete emails** (permission granted but not used)
- ⚠️ **Create calendar items** (permission granted but not used)
- ⚠️ **Modify mailbox settings** (permission granted but not used)

#### Code-Level Safeguards:
The connector implements several safeguards to prevent accidental data modification:
- **Read-only operations**: All EWS calls are explicitly read-only
- **No write methods**: Code contains no email creation, modification, or deletion functions
- **Impersonation mode**: Uses `IMPERSONATION` access type for read-only access patterns
- **Error handling**: Graceful failure without attempting data recovery operations

#### Recommended Security Improvements:

1. **Application Permissions**: Use application-level permissions instead of delegated permissions for service accounts

2. **Exchange Configuration**: Configure Exchange to limit the application's scope to specific mailboxes only

3. **Regular Auditing**: Monitor Exchange audit logs for any unexpected write operations

### General Security Best Practices

1. **Configuration File Security**: 
   - Never commit `config.json` files to version control
   - Ensure `config.json` is in your `.gitignore` file
   - Use `config.sample.json` as a template without sensitive values
   - Set appropriate file permissions: `chmod 600 config.json`
   - Configuration files are only used for local development

2. **Parameter Store Security**: 
   - Sensitive credentials are stored as SecureString parameters
   - Parameters are encrypted at rest using AWS KMS
   - Parameter access is logged in CloudTrail

3. **Network Security**: Deploy in VPC if required by your organization

4. **IAM Permissions**: Follow principle of least privilege

5. **Encryption**: Enable encryption at rest for DynamoDB table

## Cost Optimization
- **DynamoDB**: On-demand billing for processed email tracking
- **Q Business**: Based on indexed documents and queries
- **CloudWatch**: Log retention set to 30 days

## Maintenance

### Updating Configuration

1. Modify your `config.json` file
2. Deploy updated configuration:
   ```bash
   ./deploy-config.sh config.json
   ```

### Scaling

- DynamoDB scales automatically with on-demand billing
- Q Business handles indexing load automatically

### Backup and Recovery

- DynamoDB point-in-time recovery is enabled
- Q Business data can be re-indexed if needed

## Support

For issues and questions:
1. Check CloudWatch logs for error details
2. Review AWS service limits and quotas
3. Verify all configuration parameters
4. Test with a smaller email processing limit first


## Access Control and Security

### User-Specific Document Access (ACL)

The connector implements **Access Control Lists (ACL)** to ensure that each email document is only visible to the user who owns the corresponding email account. This provides secure, user-specific access to email content in Q Business.

#### How ACL Works

- **Document-Level Security**: Each indexed email document includes an ACL that restricts access to the account owner only
- **User Principal Mapping**: Documents are associated with the email account owner using their email address as the user principal
- **Automatic ACL Assignment**: ACLs are automatically applied during document creation - no manual configuration required
- **Multi-Account Support**: When processing multiple email accounts, each document is restricted to its respective account owner

#### ACL Implementation Details

For each email document, the connector adds:
```json
{
  "accessControlList": [
    {
      "access": "ALLOW",
      "principals": [
        {
          "type": "USER", 
          "value": "user@company.com"
        }
      ]
    }
  ]
}
```

#### Security Benefits

- **Data Isolation**: Users can only access their own email content through Q Business
- **Compliance**: Helps meet data privacy and security requirements
- **Multi-Tenant Support**: Safely index emails from multiple users in the same Q Business application
- **Audit Trail**: Account ownership is tracked in document metadata for auditing purposes

#### Additional Security Metadata

The connector also adds account ownership information to document attributes:
- `xchng_accountOwner`: Email address of the account owner
- `_acl_user`: User principal for ACL tracking

This metadata enables:
- **Audit Logging**: Track which user owns each document
- **Troubleshooting**: Debug access issues by identifying document ownership
- **Reporting**: Generate usage statistics per user account

## Content Format

The connector automatically detects and handles different email content formats:

- **HTML Emails**: Preserved as HTML content type with proper formatting
- **Plain Text Emails**: Stored as plain text content type
- **Mixed Content**: Email headers are formatted consistently regardless of body type
- **Large Emails**: Automatically truncated if they exceed Q Business size limits (5MB)
- **Metadata**: Email headers and properties are mapped to AWS Q Business standard fields

## Field Mappings

The connector uses the official AWS Q Business field mappings for Microsoft Exchange. Each email is indexed with the following attributes based on the [AWS Q Business Exchange field mappings](https://docs.aws.amazon.com/amazonq/latest/qbusiness-ug/exchange-field-mappings.html):

### Default Q Business Fields
| Index Field Name | Exchange Field | Type | Description |
|------------------|----------------|------|-------------|
| `_source_uri` | uri | String | Unique URI identifying the email source |
| `_created_at` | createdDateTime | Date | Email creation timestamp |
| `_last_updated_at` | lastModifiedDateTime | Date | Email last modified timestamp |
| `_category` | category | String | Document category (EMAIL) |

### Exchange Custom Index Fields
| Index Field Name | Exchange Field | Type | Description |
|------------------|----------------|------|-------------|
| `xchng_bccRecipient` | bccRecipients | String List | BCC recipient email addresses |
| `xchng_ccRecipient` | ccRecipients | String List | CC recipient email addresses |
| `xchng_hasAttachment` | hasAttachment | String | "true" if email has attachments |
| `xchng_sendDateTime` | sendDateTime | Date | Email sent timestamp |
| `xchng_importance` | importance | String | Email importance level (High, Normal, Low) |
| `xchng_from` | from | String | Sender email address |
| `xchng_to` | to | String List | Recipient email addresses |
| `xchng_receivedDateTime` | receivedDateTime | Date | Email received timestamp |
| `xchng_isRead` | isRead | String | "true" if email has been read |
| `xchng_replyTo` | replyTo | String | Reply-to email address |
| `xchng_folder` | folder | String | Exchange folder path |
| `xchng_title` | title | String | Email subject line |
| `xchng_flagStatus` | flagStatus | String | Email flag status |
| `xchng_accountOwner` | accountOwner | String | Email account owner (for ACL tracking) |
| `_acl_user` | aclUser | String | User principal for access control |

### Data Type Mapping
- **String**: Text values (single value)
- **String List**: Multiple text values (arrays)
- **Date**: ISO 8601 formatted timestamps
- **Long**: Numeric values

For complete field mapping details, see the [AWS Q Business Exchange Field Mappings documentation](https://docs.aws.amazon.com/amazonq/latest/qbusiness-ug/exchange-field-mappings.html).

## License

This project follows the same license as the parent repository.