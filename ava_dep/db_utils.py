import psycopg2
import pandas as pd
from typing import Dict, List, Any
import psycopg2.extensions 

def get_db_connection(db_params: Dict[str, str]) -> psycopg2.extensions.connection | None:
    """Establishes a connection to the PostgreSQL database."""
    conn = None
    try:
        # Convert port to int for psycopg2
        params = db_params.copy()
        params['port'] = int(params['port'])
        
        # 1. Establish the connection
        # NOTE: Connection starts in transactional mode (autocommit=False)
        conn = psycopg2.connect(**params)
        
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        if conn:
            conn.close()
        return None

def update_all_pending_to_approved(db_params: Dict[str, str], project_id: int) -> int:
    """Updates pending tasks to approved status for a specific project ID."""
    conn = get_db_connection(db_params)
    if conn:
        try:
            # This function requires transactional behavior (autocommit=False) for commit()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET qc_status = 'approved' WHERE qc_status = 'pending' AND project_id = %s;", 
                    (project_id,)
                )
                count = cur.rowcount
            conn.commit()
            return count
        except Exception as e:
            print(f"Error updating tasks: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    return 0

def get_projects(db_params: Dict[str, str]) -> Dict[int, str]:
    """Fetches all project IDs and names from the database."""
    conn = get_db_connection(db_params)
    if not conn:
        # STUB: Return dummy data if no DB connection
        return {1: "Factory-Project-Latest", 2: "Old-Batch-A"}
    try:
        # CRITICAL FIX: Set autocommit for SELECT statements to ensure non-stale reads
        conn.autocommit = True
        
        # We retrieve the data ordered by ID ascending, but the Streamlit app handles the reverse sorting.
        projects_df = pd.read_sql("SELECT project_id, name FROM projects ORDER BY project_id ASC", conn)
        return {row['project_id']: row['name'] for index, row in projects_df.iterrows()}
    except Exception as e:
        print(f"Error fetching projects: {e}")
        return {}
    finally:
        conn.close()

def get_pending_tasks(db_params: Dict[str, str], project_id: int) -> List[Dict[str, Any]]:
    """Fetches tasks that are pending QC for a given project."""
    conn = get_db_connection(db_params)
    if not conn:
        # STUB: Return dummy data if no DB connection
        if project_id == 1:
            return [
                {"task_id": 101, "name": "Video_C_task", "assignee": "user_a", "status": "completed"},
                {"task_id": 102, "name": "Video_D_task", "assignee": "user_b", "status": "validation"},
            ]
        return []
    try:
        # CRITICAL FIX: Set autocommit for SELECT statements to ensure non-stale reads
        conn.autocommit = True
        
        # Double-check isolation level just before query execution (added for aggressive freshness)
        # Note: This line might throw an error if autocommit is already set, but we leave it for debug clarity.
        # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

        pending_tasks_df = pd.read_sql(
            f"SELECT task_id, name, assignee, status FROM tasks WHERE qc_status = 'pending' AND project_id = {project_id}",
            conn
        )
        return pending_tasks_df.to_dict('records')
    except Exception as e:
        print(f"Error fetching pending tasks: {e}")
        return []
    finally:
        conn.close()
