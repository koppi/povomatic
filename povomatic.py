#!/usr/bin/env python3
# povomatic - distributed POV-Ray rendering on Kubernetes
# Copyright (C) 2026 Jakob Flierl
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
import requests
import sys
import os

def submit_job(api_url, scene, job_type, frames, priority, povray_args, ffmpeg_args, clock_initial, clock_final):
    """Submits a rendering job to the API."""
    payload = {
        "scene_file": scene,
        "type": job_type,
        "frames": frames,
        "priority": priority,
        "povray_args": povray_args,
        "ffmpeg_args": ffmpeg_args,
        "clock_initial": clock_initial,
        "clock_final": clock_final
    }
    
    try:
        response = requests.post(f"{api_url}/submit", json=payload)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully submitted rendering job.")
        print(f"Job ID: {data['job_id']}")
        print(f"Monitor status via: {api_url}/status/{data['job_id']}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to submit job: {e}")
        if 'response' in locals() and response is not None:
            print(f"Response: {response.text}")
        sys.exit(1)

def list_jobs(api_url):
    """Lists all rendering jobs."""
    try:
        response = requests.get(f"{api_url}/jobs")
        response.raise_for_status()
        jobs = response.json()
        print(f"{'ID':<5} {'Scene':<20} {'Status':<15} {'Progress':<10}")
        print("-" * 50)
        for j in jobs:
            if j['status'] in ('pending', 'rendering', 'completed'):
                print(f"{j['id']:<5} {j['scene']:<20} {j['status']:<15} {j['progress']}%")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch jobs: {e}")
        sys.exit(1)

def cancel_job(api_url, job_id):
    """Cancels a rendering job."""
    try:
        response = requests.post(f"{api_url}/cancel/{job_id}")
        response.raise_for_status()
        print(f"Successfully requested cancellation for job {job_id}.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to cancel job: {e}")
        sys.exit(1)

def clear_jobs(api_url):
    """Removes all jobs from the API."""
    try:
        response = requests.delete(f"{api_url}/jobs")
        response.raise_for_status()
        print("Successfully cleared all jobs.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to clear jobs: {e}")
        sys.exit(1)

if __name__ == "__main__":
    default_api = os.environ.get("POVOMATIC_API", "http://localhost:80")
    
    parser = argparse.ArgumentParser(description="Submit a POV-Ray rendering job to the Kubernetes cluster.")
    
    # Flags
    parser.add_argument("--list", "-l", action="store_true", help="List all jobs")
    parser.add_argument("--cancel", "-c", type=int, help="Cancel a job by ID")
    parser.add_argument("--clear", action="store_true", help="Clear all jobs from the system")
    parser.add_argument("--api-url", default=default_api, help=f"URL of the rendering API (default: {default_api})")
    
    # Required for submission
    parser.add_argument("--scene", help="Filename of the POV-Ray scene (.pov)")
    parser.add_argument("--type", choices=["still", "animation"], help="Job type (still image or animation sequence)")
    
    # Optional arguments
    parser.add_argument("--frames", type=int, default=1, help="Number of animation frames (ignored for still)")
    parser.add_argument("--priority", type=int, default=0, help="Job priority (higher is more important)")
    parser.add_argument("--clock-initial", type=float, default=0.0, help="Initial clock value")
    parser.add_argument("--clock-final", type=float, default=1.0, help="Final clock value")
    parser.add_argument("--povray-args", default="", help="Custom arguments for povray (e.g., '+W1920 +H1080')")
    parser.add_argument("--ffmpeg-args", default="", help="Custom arguments for ffmpeg (e.g., '-c:v libx264')")
    
    args = parser.parse_args()
    
    if args.list:
        list_jobs(args.api_url)
    elif args.cancel:
        cancel_job(args.api_url, args.cancel)
    elif args.clear:
        clear_jobs(args.api_url)
    elif args.scene and args.type:
        # Basic validation
        if args.type == "animation" and args.frames <= 1:
            print("Warning: animation type selected with 1 or fewer frames.")
        submit_job(args.api_url, args.scene, args.type, args.frames, args.priority, args.povray_args, args.ffmpeg_args, args.clock_initial, args.clock_final)
    else:
        parser.print_help()
        print("\nError: Either --list, --cancel, or --scene and --type must be provided.")
        sys.exit(1)
