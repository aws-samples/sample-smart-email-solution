#!/bin/bash

# Setup Parameter Store values for Exchange EWS Connector
# This script helps create the required parameters before deployment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Default values
APP_NAME="exchange-connector"
ENVIRONMENT="dev"
AWS_REGION="us-east-1"

# Function to create or update parameter
create_parameter() {
    local param_name="$1"
    local param_value="$2"
    local param_type="$3"
    local description="$4"
    
    if [ -z "$param_value" ] || [ "$param_value" = "REPLACE_WITH_YOUR_VALUE" ]; then
        print_warning "Skipping $param_name - no value provided"
        return
    fi
    
    print_status "Creating/updating parameter: $param_name"
    
    if aws ssm put-parameter \
        --name "$param_name" \
        --value "$param_value" \
        --type "$param_type" \
        --description "$description" \
        --overwrite \
        --region "$AWS_REGION" > /dev/null 2>&1; then
        print_success "✓ $param_name"
    else
        print_error "✗ Failed to create $param_name"
    fi
}

# Function to prompt for parameter value
prompt_for_value() {
    local param_name="$1"
    local description="$2"
    local is_secret="$3"
    
    echo ""
    echo "Parameter: $param_name"
    echo "Description: $description"
    
    if [ "$is_secret" = "true" ]; then
        read -s -p "Enter value (hidden): " param_value
        echo ""
    else
        read -p "Enter value: " param_value
    fi
    
    echo "$param_value"
}

# Main setup function
main() {
    echo "=========================================="
    echo "Exchange EWS Connector - Parameter Setup"
    echo "=========================================="
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --app)
                APP_NAME="$2"
                shift 2
                ;;
            --env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            --region)
                AWS_REGION="$2"
                shift 2
                ;;
            --config-file)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --app NAME          Application name (default: exchange-connector)"
                echo "  --env NAME          Environment name (default: production)"
                echo "  --region REGION     AWS region (default: us-east-1)"
                echo "  --config-file FILE  Load values from config file"
                echo "  --help              Show this help message"
                echo ""
                echo "Examples:"
                echo "  $0                                    # Interactive setup"
                echo "  $0 --config-file config.env          # Load from file"
                echo "  $0 --app my-app --env staging         # Custom app and environment"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    print_status "Configuration:"
    echo "  App Name: $APP_NAME"
    echo "  Environment: $ENVIRONMENT"
    echo "  AWS Region: $AWS_REGION"
    echo ""
    
    # Parameter prefix - using existing format for backwards compatibility
    PARAM_PREFIX="/exchange-connector/$ENVIRONMENT"
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed or not in PATH"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity > /dev/null 2>&1; then
        print_error "AWS credentials not configured or invalid"
        exit 1
    fi
    
    print_success "AWS CLI configured and credentials valid"
    
    # Load from config file if specified
    if [ -n "$CONFIG_FILE" ] && [ -f "$CONFIG_FILE" ]; then
        print_status "Loading configuration from $CONFIG_FILE"
        
        # Check if it's a JSON file
        if [[ "$CONFIG_FILE" == *.json ]]; then
            # Parse JSON file using jq or python
            if command -v jq &> /dev/null; then
                EXCHANGE_CLIENT_ID=$(jq -r '.exchange.client_id // ""' "$CONFIG_FILE")
                EXCHANGE_CLIENT_SECRET=$(jq -r '.exchange.client_secret // ""' "$CONFIG_FILE")
                EXCHANGE_TENANT_ID=$(jq -r '.exchange.tenant_id // ""' "$CONFIG_FILE")
                EXCHANGE_PRIMARY_SMTP_ADDRESS=$(jq -r '.exchange.primary_smtp_address // ""' "$CONFIG_FILE")
                QBUSINESS_APPLICATION_ID=$(jq -r '.qbusiness.application_id // ""' "$CONFIG_FILE")
                QBUSINESS_INDEX_ID=$(jq -r '.qbusiness.index_id // ""' "$CONFIG_FILE")
                QBUSINESS_DATASOURCE_ID=$(jq -r '.qbusiness.datasource_id // ""' "$CONFIG_FILE")
            elif command -v python3 &> /dev/null; then
                EXCHANGE_CLIENT_ID=$(python3 -c "import json; data=json.load(open('$CONFIG_FILE')); print(data.get('exchange', {}).get('client_id', ''))")
                EXCHANGE_CLIENT_SECRET=$(python3 -c "import json; data=json.load(open('$CONFIG_FILE')); print(data.get('exchange', {}).get('client_secret', ''))")
                EXCHANGE_TENANT_ID=$(python3 -c "import json; data=json.load(open('$CONFIG_FILE')); print(data.get('exchange', {}).get('tenant_id', ''))")
                EXCHANGE_PRIMARY_SMTP_ADDRESS=$(python3 -c "import json; data=json.load(open('$CONFIG_FILE')); print(data.get('exchange', {}).get('primary_smtp_address', ''))")
                QBUSINESS_APPLICATION_ID=$(python3 -c "import json; data=json.load(open('$CONFIG_FILE')); print(data.get('qbusiness', {}).get('application_id', ''))")
                QBUSINESS_INDEX_ID=$(python3 -c "import json; data=json.load(open('$CONFIG_FILE')); print(data.get('qbusiness', {}).get('index_id', ''))")
                QBUSINESS_DATASOURCE_ID=$(python3 -c "import json; data=json.load(open('$CONFIG_FILE')); print(data.get('qbusiness', {}).get('datasource_id', ''))")
            else
                print_error "JSON config file specified but neither jq nor python3 is available"
                exit 1
            fi
        else
            # Assume it's a .env file
            source "$CONFIG_FILE"
        fi
    fi
    
    # Collect parameter values
    echo ""
    print_status "Setting up Exchange and Q Business parameters..."
    print_warning "Leave empty to skip a parameter"
    
    # Exchange parameters
    if [ -z "$EXCHANGE_CLIENT_ID" ]; then
        EXCHANGE_CLIENT_ID=$(prompt_for_value "EXCHANGE_CLIENT_ID" "Azure AD Application Client ID" false)
    fi
    
    if [ -z "$EXCHANGE_CLIENT_SECRET" ]; then
        EXCHANGE_CLIENT_SECRET=$(prompt_for_value "EXCHANGE_CLIENT_SECRET" "Azure AD Application Client Secret" true)
    fi
    
    if [ -z "$EXCHANGE_TENANT_ID" ]; then
        EXCHANGE_TENANT_ID=$(prompt_for_value "EXCHANGE_TENANT_ID" "Azure AD Tenant ID" false)
    fi
    
    if [ -z "$EXCHANGE_PRIMARY_SMTP_ADDRESS" ]; then
        EXCHANGE_PRIMARY_SMTP_ADDRESS=$(prompt_for_value "EXCHANGE_PRIMARY_SMTP_ADDRESS" "Email addresses (comma-separated)" false)
    fi
    
    # Q Business parameters
    if [ -z "$QBUSINESS_APPLICATION_ID" ]; then
        QBUSINESS_APPLICATION_ID=$(prompt_for_value "QBUSINESS_APPLICATION_ID" "Q Business Application ID" false)
    fi
    
    if [ -z "$QBUSINESS_INDEX_ID" ]; then
        QBUSINESS_INDEX_ID=$(prompt_for_value "QBUSINESS_INDEX_ID" "Q Business Index ID" false)
    fi
    
    if [ -z "$QBUSINESS_DATASOURCE_ID" ]; then
        QBUSINESS_DATASOURCE_ID=$(prompt_for_value "QBUSINESS_DATASOURCE_ID" "Q Business Data Source ID" false)
    fi
    
    # Create parameters
    echo ""
    print_status "Creating parameters in AWS Parameter Store..."
    
    # Create parameters using existing naming convention (lowercase with hyphens)
    create_parameter "$PARAM_PREFIX/exchange-client-id" "$EXCHANGE_CLIENT_ID" "SecureString" "Azure AD Application Client ID for Exchange access"
    create_parameter "$PARAM_PREFIX/exchange-client-secret" "$EXCHANGE_CLIENT_SECRET" "SecureString" "Azure AD Application Client Secret for Exchange access"
    create_parameter "$PARAM_PREFIX/exchange-tenant-id" "$EXCHANGE_TENANT_ID" "SecureString" "Azure AD Tenant ID"
    create_parameter "$PARAM_PREFIX/exchange-primary-smtp-address" "$EXCHANGE_PRIMARY_SMTP_ADDRESS" "SecureString" "Email addresses to process (comma-separated)"
    create_parameter "$PARAM_PREFIX/qbusiness-application-id" "$QBUSINESS_APPLICATION_ID" "String" "Amazon Q Business Application ID"
    create_parameter "$PARAM_PREFIX/qbusiness-index-id" "$QBUSINESS_INDEX_ID" "String" "Amazon Q Business Index ID"
    create_parameter "$PARAM_PREFIX/qbusiness-datasource-id" "$QBUSINESS_DATASOURCE_ID" "String" "Amazon Q Business Data Source ID"
    
    echo ""
    print_success "Parameter setup completed!"
    
    echo ""
    echo "Next steps:"
    echo "1. Verify parameters: aws ssm get-parameters-by-path --path '$PARAM_PREFIX' --region $AWS_REGION"
    echo "2. Deploy the application: ./deploy.sh"
    echo "3. Monitor the deployment: copilot job logs --name exchange-sync --env $ENVIRONMENT"
}

# Run main function
main "$@"