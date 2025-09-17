#!/bin/bash

# AWS Systems Manager Parameter Store Deployment Script
# Creates secure parameters for Exchange EWS Connector from configuration file

set -e

# Configuration
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
CONFIG_FILE="${1:-config.json}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}AWS Parameter Store Deployment for Exchange EWS Connector${NC}"
echo "=========================================================="

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI is not installed${NC}"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo -e "${RED}Error: jq is not installed${NC}"
    echo "Please install jq:"
    echo "  macOS: brew install jq"
    echo "  Ubuntu/Debian: sudo apt-get install jq"
    echo "  CentOS/RHEL: sudo yum install jq"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS credentials not configured${NC}"
    echo "Please configure AWS credentials using:"
    echo "  aws configure"
    echo "  or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
    exit 1
fi

echo -e "${BLUE}AWS Region: $REGION${NC}"
echo -e "${BLUE}Environment: $ENVIRONMENT${NC}"
echo -e "${BLUE}Configuration File: $CONFIG_FILE${NC}"
echo ""

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: Configuration file '$CONFIG_FILE' not found${NC}"
    echo ""
    echo "Usage: $0 [config-file.json]"
    exit 1
fi

# Validate JSON format
if ! jq empty "$CONFIG_FILE" 2>/dev/null; then
    echo -e "${RED}Error: Invalid JSON format in '$CONFIG_FILE'${NC}"
    exit 1
fi

# Set Parameter Store prefix using environment in lowercase
PARAMETER_PREFIX="/exchange-connector/$(echo $ENVIRONMENT | tr '[:upper:]' '[:lower:]')"
echo -e "${BLUE}Parameter Store Prefix: $PARAMETER_PREFIX${NC}"
echo ""

# Function to create or update parameter
create_parameter() {
    local param_name="$1"
    local param_value="$2"
    local param_type="$3"
    local description="$4"
    
    if [ -z "$param_value" ] || [ "$param_value" = "null" ]; then
        echo -e "${YELLOW}⚠️  Skipping $param_name (no value provided)${NC}"
        return
    fi
    
    echo -e "${YELLOW}Creating/updating parameter: $param_name${NC}"
    
    aws ssm put-parameter \
        --name "$param_name" \
        --value "$param_value" \
        --type "$param_type" \
        --description "$description" \
        --overwrite \
        --region "$REGION" \
        --no-cli-pager
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully created/updated: $param_name${NC}"
    else
        echo -e "${RED}❌ Failed to create/update: $param_name${NC}"
        exit 1
    fi
}

echo -e "${YELLOW}Reading configuration from $CONFIG_FILE...${NC}"

# Extract values from JSON config
EXCHANGE_CLIENT_ID=$(jq -r '.exchange.client_id // empty' "$CONFIG_FILE")
EXCHANGE_CLIENT_SECRET=$(jq -r '.exchange.client_secret // empty' "$CONFIG_FILE")
EXCHANGE_TENANT_ID=$(jq -r '.exchange.tenant_id // empty' "$CONFIG_FILE")
EXCHANGE_PRIMARY_SMTP_ADDRESS=$(jq -r '.exchange.primary_smtp_address // empty' "$CONFIG_FILE")
EXCHANGE_SERVER=$(jq -r '.exchange.server // "outlook.office365.com"' "$CONFIG_FILE")

QBUSINESS_APPLICATION_ID=$(jq -r '.qbusiness.application_id // empty' "$CONFIG_FILE")
QBUSINESS_INDEX_ID=$(jq -r '.qbusiness.index_id // empty' "$CONFIG_FILE")
QBUSINESS_DATASOURCE_ID=$(jq -r '.qbusiness.datasource_id // empty' "$CONFIG_FILE")

echo -e "${GREEN}✓ Configuration loaded successfully${NC}"
echo ""

echo -e "${YELLOW}Creating parameters in AWS Systems Manager Parameter Store...${NC}"

# Create Exchange parameters
create_parameter \
    "$PARAMETER_PREFIX/exchange-client-id" \
    "$EXCHANGE_CLIENT_ID" \
    "SecureString" \
    "Exchange Online Application Client ID"

create_parameter \
    "$PARAMETER_PREFIX/exchange-client-secret" \
    "$EXCHANGE_CLIENT_SECRET" \
    "SecureString" \
    "Exchange Online Application Client Secret"

create_parameter \
    "$PARAMETER_PREFIX/exchange-tenant-id" \
    "$EXCHANGE_TENANT_ID" \
    "SecureString" \
    "Exchange Online Tenant ID"

create_parameter \
    "$PARAMETER_PREFIX/exchange-primary-smtp-address" \
    "$EXCHANGE_PRIMARY_SMTP_ADDRESS" \
    "String" \
    "Primary SMTP address(es) to process"

create_parameter \
    "$PARAMETER_PREFIX/exchange-server" \
    "$EXCHANGE_SERVER" \
    "String" \
    "Exchange Server URL"

# Create Q Business parameters
create_parameter \
    "$PARAMETER_PREFIX/qbusiness-application-id" \
    "$QBUSINESS_APPLICATION_ID" \
    "String" \
    "Q Business Application ID"

create_parameter \
    "$PARAMETER_PREFIX/qbusiness-index-id" \
    "$QBUSINESS_INDEX_ID" \
    "String" \
    "Q Business Index ID"

create_parameter \
    "$PARAMETER_PREFIX/qbusiness-datasource-id" \
    "$QBUSINESS_DATASOURCE_ID" \
    "String" \
    "Q Business Data Source ID"

echo ""
echo -e "${GREEN}🎉 Parameter Store deployment completed successfully!${NC}"
echo ""
echo -e "${BLUE}Created parameters with prefix: $PARAMETER_PREFIX${NC}"
echo ""
echo "To view the created parameters:"
echo "  aws ssm get-parameters-by-path --path \"$PARAMETER_PREFIX\" --recursive --region $REGION"
echo ""
echo "To update a parameter later:"
echo "  aws ssm put-parameter --name \"$PARAMETER_PREFIX/parameter-name\" --value \"new-value\" --overwrite --region $REGION"
echo ""
echo "To delete all parameters (if needed):"
echo "  aws ssm delete-parameters --names \$(aws ssm get-parameters-by-path --path \"$PARAMETER_PREFIX\" --recursive --query 'Parameters[].Name' --output text --region $REGION) --region $REGION"