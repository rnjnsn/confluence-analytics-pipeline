"""
Confluence Knowledge Base Analytics Pipeline

A Python script that extracts, enriches, and tracks comprehensive metadata 
from Confluence spaces on a weekly schedule. Creates versioned snapshots 
and maintains historical data for content governance and analytics.

Author: René Jensen
License: MIT
"""

import requests
from requests.auth import HTTPBasicAuth
import json
import csv
import os
from datetime import datetime, timedelta
import shutil
from pathlib import Path

# Configuration - Set these as environment variables or update directly
EMAIL = os.getenv("CONFLUENCE_EMAIL", "your.email@company.com")
API_TOKEN = os.getenv("CONFLUENCE_API_TOKEN", "your_api_token_here")
BASE_URL = os.getenv("CONFLUENCE_BASE_URL", "https://confluence.company.com")
SPACE_KEYS = ["SPACE1", "SPACE2"]  # List of Confluence space keys to process

# Output configuration
OUTPUT_FOLDER = "confluence_data"
KEEP_HISTORY = True
ARCHIVE_OLD_SNAPSHOTS = True
MAX_SNAPSHOTS_TO_KEEP = 12

# Workflow parameter IDs (customize for your Comala workflow setup)
OWNER_PARAM_ID = "your_owner_param_id"
RELEVANCE_PARAM_ID = "your_relevance_param_id"

def get_week_info():
    """Returns the current year, week number, and week start/end dates."""
    today = datetime.now()
    year = today.year
    week_num = today.isocalendar()[1]
    
    # Find the Monday of this week
    weekday = today.weekday()
    week_start = today - timedelta(days=weekday)
    week_end = week_start + timedelta(days=6)
    
    return {
        "year": year,
        "week_num": week_num,
        "week_start": week_start.strftime("%Y-%m-%d"),
        "week_end": week_end.strftime("%Y-%m-%d"),
        "formatted": f"{year}-W{week_num:02d}"
    }

def setup_auth():
    """Sets up authentication for the API."""  
    return HTTPBasicAuth(EMAIL, API_TOKEN), BASE_URL

def get_all_pages_in_space(space_key, auth, base_url):
    """Fetches all pages in a specified space."""
    all_pages = []
    start = 0
    limit = 100  # Max number of results per request

    while True:
        url = f"{base_url}/rest/api/content"
        params = {
            "spaceKey": space_key,
            "expand": "version,metadata.labels,history,ancestors",
            "status": "current",
            "start": start,
            "limit": limit
        }

        response = requests.get(url, params=params, auth=auth)

        if response.status_code != 200:
            print(f"Failed to retrieve pages: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            break

        data = response.json()
        results = data.get("results", [])

        for page in results:
            page["space_key"] = space_key

        all_pages.extend(results)

        if len(results) < limit:
            break

        start += limit

    return all_pages

def get_child_page_count(page_id, auth, base_url):
    """Fetches the number of child pages for a page."""
    try:
        url = f"{base_url}/rest/api/content/{page_id}/child/page"
        response = requests.get(url, auth=auth)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("size", 0)
        else:
            print(f"Failed to retrieve child pages for page {page_id}: HTTP {response.status_code}")
            return 0
    except Exception as e:
        print(f"Error retrieving child pages: {e}")
        return 0

def get_page_content_info(page_id, auth, base_url):
    """Fetches content statistics for a page."""
    try:
        # Get page content
        url = f"{base_url}/rest/api/content/{page_id}?expand=body.storage"
        response = requests.get(url, auth=auth)
        
        if response.status_code == 200:
            data = response.json()
            content = data.get("body", {}).get("storage", {}).get("value", "")
            
            # Calculate content statistics
            word_count = len(content.split())
            char_count = len(content)
            
            # Count attachments
            attachment_url = f"{base_url}/rest/api/content/{page_id}/child/attachment"
            attachment_response = requests.get(attachment_url, auth=auth)
            attachment_count = 0
            
            if attachment_response.status_code == 200:
                attachment_data = attachment_response.json()
                attachment_count = attachment_data.get("size", 0)
            
            # Count images (rough estimate)
            image_count = content.count("<img")
            
            # Count tables
            table_count = content.count("<table")
            
            return {
                "word_count": word_count,
                "char_count": char_count,
                "attachment_count": attachment_count,
                "image_count": image_count,
                "table_count": table_count
            }
        else:
            print(f"Failed to retrieve content info for page {page_id}: HTTP {response.status_code}")
            return {
                "word_count": 0,
                "char_count": 0,
                "attachment_count": 0,
                "image_count": 0,
                "table_count": 0
            }
    except Exception as e:
        print(f"Error retrieving content info: {e}")
        return {
            "word_count": 0,
            "char_count": 0,
            "attachment_count": 0,
            "image_count": 0,
            "table_count": 0
        }

def get_simplified_user_activity(page, auth, base_url):
    """Extracts user activity from already fetched page data."""
    try:
        # Extract data from the page object we already have
        
        # Get last editor info
        last_editor = ""
        if "version" in page and "by" in page["version"]:
            by_info = page["version"]["by"]
            last_editor = by_info.get("displayName", by_info.get("username", ""))
        
        # Get edit count from version number
        edit_count = page.get("version", {}).get("number", 0)
        
        return {
            "edit_count": edit_count,
            "last_editor": last_editor,
            "unique_contributors": 1  # Can't reliably determine without version history
        }
    except Exception as e:
        print(f"Error extracting user activity from page data: {e}")
        return {
            "edit_count": 0,
            "last_editor": "",
            "unique_contributors": 0
        }

def get_comala_status(page_id, auth, base_url):
    """
    Fetches Comala workflow status for a page.
    
    NOTE: This requires the Comala Document Management app for Confluence.
    If you don't use Comala, you can remove this function or adapt it
    for your workflow system.
    """
    url = f"{base_url}/rest/cw/1/content/{page_id}/status"
    response = requests.get(url, auth=auth)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 204:
        return None
    else:
        print(f"Failed to retrieve Comala workflow data for page {page_id}: HTTP {response.status_code}")
        return None

def get_comala_parameters(page_id, auth, base_url):
    """
    Fetches Comala workflow parameters for a page.
    
    NOTE: This requires the Comala Document Management app for Confluence.
    If you don't use Comala, you can remove this function or adapt it
    for your workflow system.
    """
    url = f"{base_url}/rest/cw/1/content/{page_id}/parameters"
    response = requests.get(url, auth=auth)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 204:
        return None
    else:
        print(f"Failed to retrieve Comala parameters for page {page_id}: HTTP {response.status_code}")
        return None

def extract_required_data(page, workflow_data, parameters_data=None, child_count=0, content_info=None, user_activity=None):
    """Extracts the required data from the page and workflow responses."""
    page_title = page["title"]
    
    # Construct the page URL
    page_url = f"{BASE_URL}/pages/viewpage.action?pageId={page['id']}"
    
    # Get created and modified dates
    created_date = datetime.fromisoformat(page["history"]["createdDate"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
    modified_date = datetime.fromisoformat(page["version"]["when"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
    
    # Get creator info explicitly
    creator_name = ""
    if "history" in page and "createdBy" in page["history"]:
        creator = page["history"]["createdBy"]
        creator_name = creator.get("displayName", creator.get("username", ""))
    
    # Calculate page depth based on ancestors
    page_depth = len(page.get("ancestors", []))
    
    # Extract labels
    labels = []
    if "metadata" in page and "labels" in page["metadata"]:
        labels = page["metadata"]["labels"].get("results", [])
    all_labels = ", ".join([label.get("name", "") for label in labels])
    
    # Default values for content_info and user_activity if not provided
    if content_info is None:
        content_info = {
            "word_count": 0,
            "char_count": 0,
            "attachment_count": 0,
            "image_count": 0,
            "table_count": 0
        }
    
    if user_activity is None:
        user_activity = {
            "unique_contributors": 0,
            "edit_count": 0,
            "last_editor": ""
        }
    
    result = {
        "space_key": page["space_key"],
        "page_title": page_title,
        "page_url": page_url,
        "page_created": created_date,
        "date_modified": modified_date,
        "creator_name": creator_name,
        "page_depth": page_depth,
        "child_pages": child_count,
        "workflow_name": "",
        "owner_value": "",
        "project_relevance": "",
        "labels": all_labels,
        # Content metrics
        "word_count": content_info["word_count"],
        "char_count": content_info["char_count"],
        "attachment_count": content_info["attachment_count"],
        "image_count": content_info["image_count"],
        "table_count": content_info["table_count"],
        # User activity metrics
        "unique_contributors": user_activity["unique_contributors"],
        "edit_count": user_activity["edit_count"],
        "last_editor": user_activity["last_editor"],
        # Extraction timestamp
        "extraction_date": datetime.now().strftime("%Y-%m-%d"),
        "extraction_time": datetime.now().strftime("%H:%M:%S")
    }

    if not workflow_data:
        return result

    # Extract workflow name and parameters
    # NOTE: Customize this section based on your specific workflow parameter IDs
    result["workflow_name"] = workflow_data.get("workflowName", "")
    
    if parameters_data and "workflowParameters" in parameters_data:
        for param in parameters_data["workflowParameters"]:
            if param["id"] == OWNER_PARAM_ID:
                result["owner_value"] = param.get("value", "")
            elif param["id"] == RELEVANCE_PARAM_ID:
                result["project_relevance"] = param.get("value", "")
        return result

    # Try to get parameters from workflow transitions as fallback
    transitions = workflow_data.get("state", {}).get("transitions", {})
    if "submit" in transitions and "parameters" in transitions["submit"]:
        for param in transitions["submit"]["parameters"]:
            if param["id"] == OWNER_PARAM_ID:
                result["owner_value"] = param.get("value", "")
            elif param["id"] == RELEVANCE_PARAM_ID:
                result["project_relevance"] = param.get("value", "")
    if not result["owner_value"] and "select" in transitions:
        for transition in transitions["select"]:
            if "parameters" in transition:
                for param in transition["parameters"]:
                    if param["id"] == OWNER_PARAM_ID and not result["owner_value"]:
                        result["owner_value"] = param.get("value", "")
                    elif param["id"] == RELEVANCE_PARAM_ID and not result["project_relevance"]:
                        result["project_relevance"] = param.get("value", "")

    return result

def get_fieldnames():
    """Returns the list of fieldnames for the CSV files."""
    return [
        "space_key",
        "page_title",
        "page_url",
        "page_created",
        "date_modified",
        "creator_name",
        "page_depth",
        "child_pages",
        "workflow_name",
        "owner_value",
        "project_relevance",
        "labels",
        # Content metrics
        "word_count",
        "char_count",
        "attachment_count",
        "image_count",
        "table_count",
        # User activity metrics
        "unique_contributors",
        "edit_count",
        "last_editor",
        # Extraction timestamp
        "extraction_date",
        "extraction_time"
    ]

def ensure_output_folder():
    """Creates the output folder if it doesn't exist."""
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Created output folder: {OUTPUT_FOLDER}")
    
    archive_folder = os.path.join(OUTPUT_FOLDER, "archived_snapshots")
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
        print(f"Created archive folder: {archive_folder}")

def archive_old_snapshots():
    """Move older snapshots to the archive folder."""
    if not ARCHIVE_OLD_SNAPSHOTS:
        return
    
    # Find all weekly snapshot files
    snapshot_pattern = os.path.join(OUTPUT_FOLDER, "confluence_data_*-W*.csv")
    snapshot_files = sorted(Path(OUTPUT_FOLDER).glob("confluence_data_*-W*.csv"))
    
    # If we have more than MAX_SNAPSHOTS_TO_KEEP, move the oldest ones to archive
    if len(snapshot_files) > MAX_SNAPSHOTS_TO_KEEP:
        archive_folder = os.path.join(OUTPUT_FOLDER, "archived_snapshots")
        files_to_archive = snapshot_files[:-MAX_SNAPSHOTS_TO_KEEP]
    
        for file_path in files_to_archive:
            archive_path = os.path.join(archive_folder, os.path.basename(file_path))
            shutil.move(file_path, archive_path)
            print(f"Archived old snapshot: {file_path} -> {archive_path}")

def write_to_csv(data, week_info):
    """Writes the extracted data to CSV files with weekly versioning."""
    if not data:
        print("No data to write to CSV.")
        return

    ensure_output_folder()
    fieldnames = get_fieldnames()

    current_file = os.path.join(OUTPUT_FOLDER, "confluence_data_current.csv")
    weekly_snapshot = os.path.join(OUTPUT_FOLDER, f"confluence_data_{week_info['formatted']}.csv")
    history_file = os.path.join(OUTPUT_FOLDER, "confluence_data_history.csv")

    # Write current data
    with open(current_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data successfully written to current snapshot: {current_file}")

    if KEEP_HISTORY:
        # Create weekly snapshot
        shutil.copy2(current_file, weekly_snapshot)
        print(f"Weekly snapshot saved to {weekly_snapshot}")

        # Append to history with week identifier
        history_exists = os.path.exists(history_file)
        with open(history_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not history_exists:
                writer.writeheader()
            writer.writerows(data)

        print(f"Data appended to history file: {history_file}")
    
    # Archive old snapshots if we have too many
    archive_old_snapshots()

def main():
    """Main execution function."""
    auth, base_url = setup_auth()
    start_time = datetime.now()
    week_info = get_week_info()
    all_data = []

    print(f"Starting extraction at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Processing data for week {week_info['formatted']}")
    print(f"Data will be stored in folder: {OUTPUT_FOLDER}")

    for space_key in SPACE_KEYS:
        print(f"Fetching pages from space {space_key}...")
        pages = get_all_pages_in_space(space_key, auth, base_url)
        print(f"Found {len(pages)} pages in space {space_key}.")

        for i, page in enumerate(pages):
            if i % 100 == 0 or i + 1 == len(pages):
                print(f"Processing page {i+1}/{len(pages)} in space {space_key}")

            # Get child page count
            child_count = get_child_page_count(page["id"], auth, base_url)
            
            # Get content metrics
            content_info = get_page_content_info(page["id"], auth, base_url)
            
            # Get user activity metrics from the page data we already have
            user_activity = get_simplified_user_activity(page, auth, base_url)
            
            # Get workflow data (optional - remove if not using Comala)
            workflow_data = get_comala_status(page["id"], auth, base_url)
            parameters_data = get_comala_parameters(page["id"], auth, base_url) if workflow_data else None
            
            # Extract the required data
            page_data = extract_required_data(
                page, 
                workflow_data, 
                parameters_data, 
                child_count, 
                content_info, 
                user_activity
            )

            if page_data:
                all_data.append(page_data)

    end_time = datetime.now()
    duration = end_time - start_time

    print(f"Extracted data for {len(all_data)} total pages across {len(SPACE_KEYS)} spaces.")
    print(f"Extraction completed in {duration.total_seconds():.2f} seconds.")

    write_to_csv(all_data, week_info)

    print(f"Script completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Weekly snapshot created for {week_info['formatted']}")

if __name__ == "__main__":
    main()
