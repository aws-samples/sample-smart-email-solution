#!/usr/bin/env python3
"""
Sync Job Manager Utility
Provides commands to manage distributed sync jobs and container registrations
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.config import Config
from modules.qbusiness_client import QBusinessClient
from modules.sync_job_coordinator import SyncJobCoordinator

def list_active_sync_jobs(coordinator):
    """List all active sync jobs"""
    print("🔍 Checking for active sync jobs...")
    
    active_job = coordinator.get_active_sync_job()
    
    if active_job:
        job_id = active_job.get('job_id', 'Unknown')
        owner = active_job.get('owner_container_name', 'Unknown')
        created_at = active_job.get('created_at', 'Unknown')
        last_heartbeat = active_job.get('last_heartbeat', 'Unknown')
        
        print(f"✅ Found active sync job:")
        print(f"   Job ID: {job_id}")
        print(f"   Owner: {owner}")
        print(f"   Created: {created_at}")
        print(f"   Last Heartbeat: {last_heartbeat}")
        
        # List active containers for this sync job
        active_containers = coordinator.get_active_containers(job_id)
        print(f"   Active Containers: {len(active_containers)}")
        
        for i, container in enumerate(active_containers, 1):
            container_name = container.get('container_name', 'Unknown')
            registered_at = container.get('registered_at', 'Unknown')
            last_heartbeat = container.get('last_heartbeat', 'Unknown')
            print(f"     {i}. {container_name}")
            print(f"        Registered: {registered_at}")
            print(f"        Last Heartbeat: {last_heartbeat}")
    else:
        print("ℹ️  No active sync jobs found")

def cleanup_stale_registrations(coordinator):
    """Clean up stale sync job and container registrations"""
    print("🧹 Cleaning up stale registrations...")
    
    try:
        coordinator.cleanup_stale_registrations()
        print("✅ Cleanup completed successfully")
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")

def force_stop_sync_jobs(qbusiness_client):
    """Force stop all running Q Business sync jobs"""
    print("🛑 Force stopping all running sync jobs...")
    
    try:
        success = qbusiness_client.force_stop_all_sync_jobs()
        if success:
            print("✅ All sync jobs stopped successfully")
        else:
            print("⚠️  Some sync jobs may still be running")
    except Exception as e:
        print(f"❌ Failed to stop sync jobs: {e}")

def show_sync_job_status(qbusiness_client):
    """Show Q Business sync job status"""
    print("📊 Checking Q Business sync job status...")
    
    try:
        has_running = qbusiness_client.has_running_sync_jobs()
        if not has_running:
            print("ℹ️  No running Q Business sync jobs found")
    except Exception as e:
        print(f"❌ Failed to check sync job status: {e}")

def monitor_sync_jobs(coordinator, qbusiness_client, duration=60):
    """Monitor sync jobs for a specified duration"""
    print(f"👀 Monitoring sync jobs for {duration} seconds...")
    print("Press Ctrl+C to stop monitoring")
    
    import time
    
    start_time = time.time()
    
    try:
        while time.time() - start_time < duration:
            print(f"\n⏰ {datetime.now().strftime('%H:%M:%S')} - Status Check")
            print("-" * 50)
            
            # Check coordinator status
            active_job = coordinator.get_active_sync_job()
            if active_job:
                job_id = active_job.get('job_id', 'Unknown')
                owner = active_job.get('owner_container_name', 'Unknown')
                
                active_containers = coordinator.get_active_containers(job_id)
                print(f"📋 Active Sync Job: {job_id} (Owner: {owner})")
                print(f"🔄 Active Containers: {len(active_containers)}")
                
                for container in active_containers:
                    name = container.get('container_name', 'Unknown')
                    heartbeat = container.get('last_heartbeat', 'Unknown')
                    print(f"   - {name}: {heartbeat}")
            else:
                print("ℹ️  No active sync jobs in coordinator")
            
            # Check Q Business status
            try:
                has_running = qbusiness_client.has_running_sync_jobs()
                if not has_running:
                    print("ℹ️  No running Q Business sync jobs")
            except:
                print("⚠️  Could not check Q Business sync job status")
            
            # Wait 10 seconds before next check
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped by user")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Manage distributed Q Business sync jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sync_job_manager.py list                    # List active sync jobs
  python sync_job_manager.py cleanup                 # Clean up stale registrations
  python sync_job_manager.py force-stop              # Force stop all sync jobs
  python sync_job_manager.py status                  # Show Q Business sync job status
  python sync_job_manager.py monitor --duration 120  # Monitor for 2 minutes
        """
    )
    
    parser.add_argument(
        'command',
        choices=['list', 'cleanup', 'force-stop', 'status', 'monitor'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Duration for monitor command (seconds, default: 60)'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("SYNC JOB MANAGER")
    print("=" * 80)
    
    try:
        # Initialize components
        config = Config()
        qbusiness_client = QBusinessClient(config)
        coordinator = SyncJobCoordinator(config, qbusiness_client)
        
        print(f"🔧 Initialized with table: {coordinator.sync_table_name}")
        print(f"🏷️  Container ID: {coordinator.container_name}")
        print()
        
        # Execute command
        if args.command == 'list':
            list_active_sync_jobs(coordinator)
            
        elif args.command == 'cleanup':
            cleanup_stale_registrations(coordinator)
            
        elif args.command == 'force-stop':
            force_stop_sync_jobs(qbusiness_client)
            
        elif args.command == 'status':
            show_sync_job_status(qbusiness_client)
            
        elif args.command == 'monitor':
            if args.duration < 10:
                print("❌ Duration must be at least 10 seconds")
                sys.exit(1)
            monitor_sync_jobs(coordinator, qbusiness_client, args.duration)
        
        print("\n✅ Command completed successfully")
        
    except KeyboardInterrupt:
        print("\n⚠️  Command interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n❌ Command failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()