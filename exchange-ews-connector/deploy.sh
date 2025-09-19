#!/bin/bash

# Exchange EWS Connector - AWS Copilot Deployment Script
# This script helps deploy the Exchange connector to AWS using Copilot

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="exchange-connector"
ENVIRONMENT="dev"
DEPLOYMENT_MODE="single"  # Options: single, split

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

# Function to check if copilot is installed
check_copilot() {
    if ! command -v copilot &> /dev/null; then
        print_error "AWS Copilot CLI is not installed. Please install it first:"
        echo "  curl -Lo copilot https://github.com/aws/copilot-cli/releases/latest/download/copilot-linux"
        echo "  chmod +x copilot && sudo mv copilot /usr/local/bin"
        exit 1
    fi
    print_success "AWS Copilot CLI is installed"
}

# Function to initialize Copilot application
init_app() {
    print_status "Initializing Copilot application: $APP_NAME"
    
    if [ ! -f "copilot/.workspace" ]; then
        copilot app init $APP_NAME
        print_success "Copilot application initialized"
    else
        print_warning "Copilot application already initialized"
    fi
}

# Function to deploy environment
deploy_environment() {
    print_status "Deploying environment: $ENVIRONMENT"
    copilot env init --name $ENVIRONMENT
    copilot env deploy --name $ENVIRONMENT
    print_success "Environment deployed"
}

# Function to check parameter store values
check_parameters() {
    print_status "Checking Parameter Store values..."
    
    # Check if parameters exist - using existing format for backwards compatibility
    PARAM_PREFIX="/exchange-connector/$ENVIRONMENT"
    REQUIRED_PARAMS=(
        "exchange-client-id"
        "exchange-client-secret" 
        "exchange-tenant-id"
        "exchange-primary-smtp-address"
        "qbusiness-application-id"
        "qbusiness-index-id"
        "qbusiness-datasource-id"
    )
    
    MISSING_PARAMS=()
    
    for param in "${REQUIRED_PARAMS[@]}"; do
        if ! aws ssm get-parameter --name "$PARAM_PREFIX/$param" --region "${AWS_DEFAULT_REGION:-us-east-1}" > /dev/null 2>&1; then
            MISSING_PARAMS+=("$param")
        fi
    done
    
    if [ ${#MISSING_PARAMS[@]} -gt 0 ]; then
        print_error "Missing required parameters:"
        for param in "${MISSING_PARAMS[@]}"; do
            echo "  - $PARAM_PREFIX/$param"
        done
        echo ""
        print_status "Run the parameter setup script first:"
        echo "  ./setup-parameters.sh --app $APP_NAME --env $ENVIRONMENT"
        exit 1
    fi
    
    print_success "All required parameters found in Parameter Store"
}

# Function to deploy single container
deploy_single() {
    print_status "Deploying single container mode (Backend Service)"
    copilot svc init exchange-sync
    copilot svc deploy --name exchange-sync --env $ENVIRONMENT
    print_success "Single container deployed as Backend Service"
}

# Function to deploy split containers
deploy_split() {
    print_status "Deploying split container mode"
    print_warning "For split mode, use ./generate-workers.sh <number> to create worker configurations first"
    print_status "Example: ./generate-workers.sh 3"
    
    # Check if any worker configurations exist
    if ls copilot/exchange-sync-worker-* 1> /dev/null 2>&1; then
        print_status "Found existing worker configurations, deploying them..."
        for worker_dir in copilot/exchange-sync-worker-*; do
            if [ -d "$worker_dir" ]; then
                worker_name=$(basename "$worker_dir")
                print_status "Deploying $worker_name"
                copilot svc init "$worker_name"
                copilot svc deploy --name "$worker_name" --env "$ENVIRONMENT"
            fi
        done
        print_success "Split containers deployed as Backend Services"
    else
        print_error "No worker configurations found. Run ./generate-workers.sh <number> first"
        exit 1
    fi
}

# Function to show deployment status
show_status() {
    print_status "Deployment status:"
    copilot svc ls
}

# Main deployment logic
main() {
    echo "=========================================="
    echo "Exchange EWS Connector - Copilot Deploy"
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
            --mode)
                DEPLOYMENT_MODE="$2"
                shift 2
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --app NAME      Application name (default: exchange-connector)"
                echo "  --env NAME      Environment name (default: production)"
                echo "  --mode MODE     Deployment mode: single|split (default: single)"
                echo "  --help          Show this help message"
                echo ""
                echo "Examples:"
                echo "  $0                                    # Deploy single container"
                echo "  $0 --mode split                       # Deploy split containers (requires ./generate-workers.sh first)"
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
    echo "  Deployment Mode: $DEPLOYMENT_MODE"
    echo ""
    
    # Validate deployment mode
    if [[ "$DEPLOYMENT_MODE" != "single" && "$DEPLOYMENT_MODE" != "split" ]]; then
        print_error "Invalid deployment mode: $DEPLOYMENT_MODE. Use 'single' or 'split'"
        exit 1
    fi
    
    # Check prerequisites
    check_copilot
    
    # Initialize application
    init_app
    
    # Deploy environment
    deploy_environment
    
    # Check parameters
    check_parameters
    
    # Deploy based on mode
    if [[ "$DEPLOYMENT_MODE" == "single" ]]; then
        deploy_single
    else
        deploy_split
    fi
    
    # Show status
    show_status
    
    print_success "Deployment completed!"
    echo ""
    echo "Next steps:"
    echo "1. Monitor the service: copilot svc logs --name exchange-sync --env $ENVIRONMENT"
    echo "2. Check service status: copilot svc status --name exchange-sync --env $ENVIRONMENT"
    echo "3. Check health endpoint: curl https://your-service-endpoint/health"
    echo "4. Verify DynamoDB table has processed email records"
    echo "5. Check Q Business index for synchronized documents"
}

# Run main function
main "$@"