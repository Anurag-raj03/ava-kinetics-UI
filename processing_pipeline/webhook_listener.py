from flask import Flask, request, jsonify
import subprocess
import json
import logging
import os
from pathlib import Path # Use Path for robust, cross-platform path handling

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- Deployment-Safe Project Root Definition ---
# Defines the project root relative to the script's location.
# Assumes this webhook script is located in the root directory of the 'ava_kinetics' project.
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = str(CURRENT_DIR) 
logger.info(f"Dynamically determined PROJECT_ROOT: {PROJECT_ROOT}")


# Define the path to the post_annotation_service.py script
SCRIPT_RELATIVE_PATH = "services/post_annotation_service.py"
SCRIPT_PATH = os.path.join(PROJECT_ROOT, SCRIPT_RELATIVE_PATH)


@app.route('/webhook', methods=['POST'])
def cvat_webhook():
    """ This endpoint listens for 'update:task' or 'update:job' events and triggers post-annotation service on completion. """
    if not request.is_json:
        return jsonify({"status": "error", "message": "Request must be JSON."}), 400

    payload = request.get_json()
    logger.info(f"Received webhook payload: {json.dumps(payload, indent=2)}")

    event = payload.get("event")
    task_id = None
    assignee = "N/A"

    # --- Logic to find the completed Task/Job ID ---
    if event == "update:task":
        task_info = payload.get("task", {})
        if task_info.get("status") == "completed":
            task_id = task_info.get("id")
            assignee = (task_info.get("assignee") or {}).get("username", "N/A") 

    elif event == "update:job":
        job_info = payload.get("job", {})
        if job_info.get("state") == "completed": 
            task_id = job_info.get("task_id")
            # Get the reliable assignee from the job payload
            assignee = (job_info.get("assignee") or {}).get("username", (payload.get("sender") or {}).get("username", "N/A"))
            
    # --- End Logic ---

    if task_id:
        logger.info(f"✅ Job/Task {task_id} completed by {assignee}. Triggering post-annotation service...")

        try:
            # Trigger the post_annotation_service.py script as a background process
            subprocess.Popen(
                [
                    "python", 
                    # Use the relative path combined with the correct working directory
                    SCRIPT_RELATIVE_PATH, 
                    "--task-id", str(task_id),
                    "--assignee", assignee 
                ],
                # CRITICAL: Setting cwd ensures the 'python' executable finds the script 
                # and the script can resolve its own relative imports (e.g., from processing_pipeline)
                cwd=PROJECT_ROOT, 
                
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return jsonify({"status": "success", "message": "Post-annotation service triggered."}), 200
        except Exception as e:
            logger.error(f"Failed to trigger post-annotation service: {e}")
            return jsonify({"status": "error", "message": "Failed to trigger service."}), 500

    return jsonify({"status": "ignored", "message": f"Event was not a completion event (received: {event})."}), 200

if __name__ == '__main__':
    # Final check on paths before starting
    if not os.path.exists(SCRIPT_PATH):
         logger.error(f"ERROR: Post-annotation script not found at: {SCRIPT_PATH}. Check your project structure.")
    
    app.run(host='0.0.0.0', port=5001, debug=True)