from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.catalog import TableInfo, SchemaInfo, ColumnInfo, CatalogInfo
from databricks.sdk.service.sql import StatementResponse, StatementState
from typing import Dict, Any, List
import os
import json
import time
from dotenv import load_dotenv

# Load environment variables from .env file when the module is imported
load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")

# Auth: PAT (legacy) or OAuth M2M (recommended)
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
DATABRICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")

if not DATABRICKS_HOST:
    raise ImportError(
        "DATABRICKS_HOST must be set in environment variables or .env file."
    )

has_pat = bool(DATABRICKS_TOKEN)
has_oauth = bool(DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET)

if not has_pat and not has_oauth:
    raise ImportError(
        "Authentication required. Set either:\n"
        "  - DATABRICKS_TOKEN (legacy PAT auth), or\n"
        "  - DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET (OAuth M2M, recommended)"
    )

# Configure and initialize the global SDK client
# SDK unified auth auto-detects credentials from environment variables
sdk_config = Config(
    host=DATABRICKS_HOST,
    http_timeout_seconds=30,
    retry_timeout_seconds=60
)
sdk_client = WorkspaceClient(config=sdk_config)

# Cache for job information to avoid redundant API calls
_job_cache = {}
_notebook_cache = {}

def _format_column_details_md(columns: List[ColumnInfo]) -> List[str]:
    """
    Formats a list of ColumnInfo objects into a list of Markdown strings.
    """
    markdown_lines = []
    if not columns:
        markdown_lines.append("  - *No column information available.*")
        return markdown_lines

    for col in columns:
        if not isinstance(col, ColumnInfo):
            print(f"Warning: Encountered an unexpected item in columns list: {type(col)}. Skipping.")
            continue
        col_type = col.type_text or (col.type_name.value if col.type_name and hasattr(col.type_name, 'value') else "N/A")
        nullable_status = "nullable" if col.nullable else "not nullable"
        col_description = f": {col.comment}" if col.comment else ""
        markdown_lines.append(f"  - **{col.name}** (`{col_type}`, {nullable_status}){col_description}")
    return markdown_lines

def _get_job_info_cached(job_id: str) -> Dict[str, Any]:
    """Get job information with caching to avoid redundant API calls"""
    if job_id not in _job_cache:
        try:
            job_info = sdk_client.jobs.get(job_id=job_id)
            _job_cache[job_id] = {
                'name': job_info.settings.name if job_info.settings.name else f"Job {job_id}",
                'tasks': []
            }
            
            # Pre-process all tasks to build notebook mapping
            if job_info.settings.tasks:
                for task in job_info.settings.tasks:
                    if hasattr(task, 'notebook_task') and task.notebook_task:
                        task_info = {
                            'task_key': task.task_key,
                            'notebook_path': task.notebook_task.notebook_path
                        }
                        _job_cache[job_id]['tasks'].append(task_info)
                        
        except Exception as e:
            print(f"Error fetching job {job_id}: {e}")
            _job_cache[job_id] = {
                'name': f"Job {job_id}",
                'tasks': [],
                'error': str(e)
            }
    
    return _job_cache[job_id]

def _get_notebook_id_cached(notebook_path: str) -> str:
    """Get notebook ID with caching to avoid redundant API calls"""
    if notebook_path not in _notebook_cache:
        try:
            notebook_details = sdk_client.workspace.get_status(notebook_path)
            _notebook_cache[notebook_path] = str(notebook_details.object_id)
        except Exception as e:
            print(f"Error fetching notebook {notebook_path}: {e}")
            _notebook_cache[notebook_path] = None
    
    return _notebook_cache[notebook_path]

def _resolve_notebook_info_optimized(notebook_id: str, job_id: str) -> Dict[str, Any]:
    """
    Optimized version that resolves notebook info using cached job data.
    Returns dict with notebook_path, notebook_name, job_name, and task_key.
    """
    result = {
        'notebook_id': notebook_id,
        'notebook_path': f"notebook_id:{notebook_id}",
        'notebook_name': f"notebook_id:{notebook_id}",
        'job_id': job_id,
        'job_name': f"Job {job_id}",
        'task_key': None
    }
    
    # Get cached job info
    job_info = _get_job_info_cached(job_id)
    result['job_name'] = job_info['name']
    
    # Look for notebook in job tasks
    for task_info in job_info['tasks']:
        notebook_path = task_info['notebook_path']
        cached_notebook_id = _get_notebook_id_cached(notebook_path)
        
        if cached_notebook_id == notebook_id:
            result['notebook_path'] = notebook_path
            result['notebook_name'] = notebook_path.split('/')[-1]
            result['task_key'] = task_info['task_key']
            break
    
    return result

def _format_notebook_info_optimized(notebook_info: Dict[str, Any]) -> str:
    """
    Formats notebook information using pre-resolved data.
    """
    lines = []
    
    if notebook_info['notebook_path'].startswith('/'):
        lines.append(f"**`{notebook_info['notebook_name']}`**")
        lines.append(f"  - **Path**: `{notebook_info['notebook_path']}`")
    else:
        lines.append(f"**{notebook_info['notebook_name']}**")
    
    lines.append(f"  - **Job**: {notebook_info['job_name']} (ID: {notebook_info['job_id']})")
    if notebook_info['task_key']:
        lines.append(f"  - **Task**: {notebook_info['task_key']}")
    
    return "\n".join(lines)

def _process_lineage_results(lineage_query_output: Dict[str, Any], main_table_full_name: str) -> Dict[str, Any]:
    """
    Optimized version of lineage processing that batches API calls and uses caching.
    """
    print("Processing lineage results with optimization...")
    start_time = time.time()
    
    processed_data: Dict[str, Any] = {
        "upstream_tables": [],
        "downstream_tables": [],
        "notebooks_reading": [],
        "notebooks_writing": []
    }
    
    if not lineage_query_output or lineage_query_output.get("status") != "success" or not isinstance(lineage_query_output.get("data"), list):
        print("Warning: Lineage query output is invalid or not successful. Returning empty lineage.")
        return processed_data

    upstream_set = set()
    downstream_set = set()
    notebooks_reading_dict = {}
    notebooks_writing_dict = {}
    
    # Collect all unique job IDs first for batch processing
    unique_job_ids = set()
    notebook_job_pairs = []
    
    for row in lineage_query_output["data"]:
        source_table = row.get("source_table_full_name")
        target_table = row.get("target_table_full_name")
        entity_metadata = row.get("entity_metadata")
        
        # Parse entity metadata
        notebook_id = None
        job_id = None
        
        if entity_metadata:
            try:
                if isinstance(entity_metadata, str):
                    metadata_dict = json.loads(entity_metadata)
                else:
                    metadata_dict = entity_metadata
                    
                notebook_id = metadata_dict.get("notebook_id")
                job_info = metadata_dict.get("job_info")
                if job_info:
                    job_id = job_info.get("job_id")
            except (json.JSONDecodeError, AttributeError):
                pass
        
        # Process table-to-table lineage
        if source_table == main_table_full_name and target_table and target_table != main_table_full_name:
            downstream_set.add(target_table)
        elif target_table == main_table_full_name and source_table and source_table != main_table_full_name:
            upstream_set.add(source_table)
        
        # Collect notebook-job pairs for batch processing
        if notebook_id and job_id:
            unique_job_ids.add(job_id)
            notebook_job_pairs.append({
                'notebook_id': notebook_id,
                'job_id': job_id,
                'source_table': source_table,
                'target_table': target_table
            })
    
    # Pre-load all job information in parallel (this is where the optimization happens)
    print(f"Pre-loading {len(unique_job_ids)} unique jobs...")
    batch_start = time.time()
    
    for job_id in unique_job_ids:
        _get_job_info_cached(job_id)  # This will cache the job info
    
    batch_time = time.time() - batch_start
    print(f"Job batch loading took {batch_time:.2f} seconds")
    
    # Now process all notebook-job pairs using cached data
    print(f"Processing {len(notebook_job_pairs)} notebook entries...")
    for pair in notebook_job_pairs:
        notebook_info = _resolve_notebook_info_optimized(pair['notebook_id'], pair['job_id'])
        formatted_info = _format_notebook_info_optimized(notebook_info)
        
        if pair['source_table'] == main_table_full_name:
            notebooks_reading_dict[pair['notebook_id']] = formatted_info
        elif pair['target_table'] == main_table_full_name:
            notebooks_writing_dict[pair['notebook_id']] = formatted_info
    
    processed_data["upstream_tables"] = sorted(list(upstream_set))
    processed_data["downstream_tables"] = sorted(list(downstream_set))
    processed_data["notebooks_reading"] = sorted(list(notebooks_reading_dict.values()))
    processed_data["notebooks_writing"] = sorted(list(notebooks_writing_dict.values()))
    
    total_time = time.time() - start_time
    print(f"Total lineage processing took {total_time:.2f} seconds")
    
    return processed_data

def clear_lineage_cache():
    """Clear the job and notebook caches to free memory"""
    global _job_cache, _notebook_cache
    _job_cache = {}
    _notebook_cache = {}
    print("Cleared lineage caches")

def _get_table_lineage(table_full_name: str) -> Dict[str, Any]:
    """
    Retrieves table lineage information for a given table using the global SDK client
    and global SQL warehouse ID. Now includes notebook and job information with enhanced details.
    """
    if not DATABRICKS_SQL_WAREHOUSE_ID: # Check before attempting query
        return {"status": "error", "error": "DATABRICKS_SQL_WAREHOUSE_ID is not set. Cannot fetch lineage."}

    lineage_sql_query = f"""
    SELECT source_table_full_name, target_table_full_name, entity_type, entity_id, 
           entity_run_id, entity_metadata, created_by, event_time
    FROM system.access.table_lineage
    WHERE source_table_full_name = '{table_full_name}' OR target_table_full_name = '{table_full_name}'
    ORDER BY event_time DESC LIMIT 100;
    """
    print(f"Fetching and processing lineage for table: {table_full_name}")
    # execute_databricks_sql will now use the global warehouse_id
    raw_lineage_output = execute_databricks_sql(lineage_sql_query, wait_timeout='50s') 
    return _process_lineage_results(raw_lineage_output, table_full_name)

def _format_single_table_md(table_info: TableInfo, base_heading_level: int, display_columns: bool) -> List[str]:
    """
    Formats the details for a single TableInfo object into a list of Markdown strings.
    Uses a base_heading_level to control Markdown header depth for hierarchical display.
    """
    table_markdown_parts = []
    table_header_prefix = "#" * base_heading_level
    sub_header_prefix = "#" * (base_heading_level + 1)

    table_markdown_parts.append(f"{table_header_prefix} Table: **{table_info.full_name}**")

    if table_info.comment:
        table_markdown_parts.extend(["", f"**Description**: {table_info.comment}"])
    elif base_heading_level == 1:
        table_markdown_parts.extend(["", "**Description**: No description provided."])
    
    # Process and add partition columns
    partition_column_names: List[str] = []
    if table_info.columns:
        temp_partition_cols: List[tuple[str, int]] = []
        for col in table_info.columns:
            if col.partition_index is not None:
                temp_partition_cols.append((col.name, col.partition_index))
        if temp_partition_cols:
            temp_partition_cols.sort(key=lambda x: x[1])
            partition_column_names = [name for name, index in temp_partition_cols]

    if partition_column_names:
        table_markdown_parts.extend(["", f"{sub_header_prefix} Partition Columns"])
        table_markdown_parts.extend([f"- `{col_name}`" for col_name in partition_column_names])
    elif base_heading_level == 1:
        table_markdown_parts.extend(["", f"{sub_header_prefix} Partition Columns", "- *This table is not partitioned or partition key information is unavailable.*"])

    if display_columns:
        table_markdown_parts.extend(["", f"{sub_header_prefix} Table Columns"])
        if table_info.columns:
            table_markdown_parts.extend(_format_column_details_md(table_info.columns))
        else:
            table_markdown_parts.append("  - *No column information available.*")
            
    return table_markdown_parts

def execute_databricks_sql(sql_query: str, wait_timeout: str = '50s') -> Dict[str, Any]:
    """
    Executes a SQL query on Databricks using the global SDK client and global SQL warehouse ID.
    """
    if not DATABRICKS_SQL_WAREHOUSE_ID:
        return {"status": "error", "error": "DATABRICKS_SQL_WAREHOUSE_ID is not set. Cannot execute SQL query."}
    
    try:
        print(f"Executing SQL on warehouse {DATABRICKS_SQL_WAREHOUSE_ID} (timeout: {wait_timeout}):\n{sql_query[:200]}..." + (" (truncated)" if len(sql_query) > 200 else ""))
        response: StatementResponse = sdk_client.statement_execution.execute_statement(
            statement=sql_query,
            warehouse_id=DATABRICKS_SQL_WAREHOUSE_ID, # Use global warehouse ID
            wait_timeout=wait_timeout
        )

        if response.status and response.status.state == StatementState.SUCCEEDED:
            if response.result and response.result.data_array:
                column_names = [col.name for col in response.manifest.schema.columns] if response.manifest and response.manifest.schema and response.manifest.schema.columns else []
                results = [dict(zip(column_names, row)) for row in response.result.data_array]
                return {"status": "success", "row_count": len(results), "data": results}
            else:
                return {"status": "success", "row_count": 0, "data": [], "message": "Query succeeded but returned no data."}
        elif response.status:
            error_message = response.status.error.message if response.status.error else "No error details provided."
            return {"status": "failed", "error": f"Query execution failed with state: {response.status.state.value}", "details": error_message}
        else:
            return {"status": "failed", "error": "Query execution status unknown."}
    except Exception as e:
        return {"status": "error", "error": f"An error occurred during SQL execution: {str(e)}"}

def get_uc_table_details(full_table_name: str, include_lineage: bool = False) -> str:
    """
    Fetches table metadata and optionally lineage, then formats it into a Markdown string.
    Uses the _format_single_table_md helper for core table structure.
    """
    print(f"Fetching metadata for {full_table_name}...")
    
    try:
        table_info: TableInfo = sdk_client.tables.get(full_name=full_table_name)
    except Exception as e:
        error_details = str(e)
        return f"""# Error: Could Not Retrieve Table Details
**Table:** `{full_table_name}`
**Problem:** Failed to fetch the complete metadata for this table.
**Details:**
```
{error_details}
```"""

    markdown_parts = _format_single_table_md(table_info, base_heading_level=1, display_columns=True)

    if include_lineage:
        markdown_parts.extend(["", "## Lineage Information"])
        if not DATABRICKS_SQL_WAREHOUSE_ID:
            markdown_parts.append("- *Lineage fetching skipped: `DATABRICKS_SQL_WAREHOUSE_ID` environment variable is not set.*")
        else:
            print(f"Fetching lineage for {full_table_name}...")
            lineage_info = _get_table_lineage(full_table_name)
            
            has_upstream = lineage_info and isinstance(lineage_info.get("upstream_tables"), list) and lineage_info["upstream_tables"]
            has_downstream = lineage_info and isinstance(lineage_info.get("downstream_tables"), list) and lineage_info["downstream_tables"]
            has_notebooks_reading = lineage_info and isinstance(lineage_info.get("notebooks_reading"), list) and lineage_info["notebooks_reading"]
            has_notebooks_writing = lineage_info and isinstance(lineage_info.get("notebooks_writing"), list) and lineage_info["notebooks_writing"]

            if has_upstream:
                markdown_parts.extend(["", "### Upstream Tables (tables this table reads from):"])
                markdown_parts.extend([f"- `{table}`" for table in lineage_info["upstream_tables"]])
            
            if has_downstream:
                markdown_parts.extend(["", "### Downstream Tables (tables that read from this table):"])
                markdown_parts.extend([f"- `{table}`" for table in lineage_info["downstream_tables"]])
            
            if has_notebooks_reading:
                markdown_parts.extend(["", "### Notebooks Reading from this Table:"])
                for notebook in lineage_info["notebooks_reading"]:
                    markdown_parts.extend([f"- {notebook}", ""])
            
            if has_notebooks_writing:
                markdown_parts.extend(["", "### Notebooks Writing to this Table:"])
                for notebook in lineage_info["notebooks_writing"]:
                    markdown_parts.extend([f"- {notebook}", ""])
            
            if not any([has_upstream, has_downstream, has_notebooks_reading, has_notebooks_writing]):
                if lineage_info and lineage_info.get("status") == "error" and lineage_info.get("error"):
                     markdown_parts.extend(["", "*Note: Could not retrieve complete lineage information.*", f"> *Lineage fetch error: {lineage_info.get('error')}*"])
                elif lineage_info and lineage_info.get("status") != "success" and lineage_info.get("error"):
                    markdown_parts.extend(["", "*Note: Could not retrieve complete lineage information.*", f"> *Lineage fetch error: {lineage_info.get('error')}*"])
                else:
                    markdown_parts.append("- *No table, notebook, or job dependencies found or lineage fetch was not fully successful.*")
    else:
        markdown_parts.extend(["", "## Lineage Information", "- *Lineage fetching skipped as per request.*"])

    return "\n".join(markdown_parts)

def get_uc_schema_details(catalog_name: str, schema_name: str, include_columns: bool = False) -> str:
    """
    Fetches detailed information for a specific schema, optionally including its tables and their columns.
    Uses the global SDK client and the _format_single_table_md helper with appropriate heading levels.
    """
    full_schema_name = f"{catalog_name}.{schema_name}"
    markdown_parts = [f"# Schema Details: **{full_schema_name}**"]

    try:
        print(f"Fetching details for schema: {full_schema_name}...")
        schema_info: SchemaInfo = sdk_client.schemas.get(full_name=full_schema_name)

        description = schema_info.comment if schema_info.comment else "No description provided."
        markdown_parts.append(f"**Description**: {description}")
        markdown_parts.append("")

        markdown_parts.append(f"## Tables in Schema `{schema_name}`")
            
        tables_iterable = sdk_client.tables.list(catalog_name=catalog_name, schema_name=schema_name)
        tables_list = list(tables_iterable)

        if not tables_list:
            markdown_parts.append("- *No tables found in this schema.*")
        else:
            for i, table_info in enumerate(tables_list):
                if not isinstance(table_info, TableInfo):
                    print(f"Warning: Encountered an unexpected item in tables list: {type(table_info)}")
                    continue
                
                markdown_parts.extend(_format_single_table_md(
                    table_info, 
                    base_heading_level=3,
                    display_columns=include_columns
                ))
                if i < len(tables_list) - 1:
                    markdown_parts.append("\n=============\n")
                else:
                    markdown_parts.append("")

    except Exception as e:
        error_message = f"Failed to retrieve details for schema '{full_schema_name}': {str(e)}"
        print(f"Error in get_uc_schema_details: {error_message}")
        return f"""# Error: Could Not Retrieve Schema Details
**Schema:** `{full_schema_name}`
**Problem:** An error occurred while attempting to fetch schema information.
**Details:**
```
{error_message}
```"""

    return "\n".join(markdown_parts)

def get_uc_catalog_details(catalog_name: str) -> str:
    """
    Fetches and formats a summary of all schemas within a given catalog
    using the global SDK client.
    """
    markdown_parts = [f"# Catalog Summary: **{catalog_name}**", ""]
    schemas_found_count = 0
    
    try:
        print(f"Fetching schemas for catalog: {catalog_name} using global sdk_client...")
        # The sdk_client is globally defined in this module
        schemas_iterable = sdk_client.schemas.list(catalog_name=catalog_name)
        
        # Convert iterator to list to easily check if empty and get a count
        schemas_list = list(schemas_iterable) 

        if not schemas_list:
            markdown_parts.append(f"No schemas found in catalog `{catalog_name}`.")
            return "\n".join(markdown_parts)

        schemas_found_count = len(schemas_list)
        markdown_parts.append(f"Showing top {schemas_found_count} schemas found in catalog `{catalog_name}`:")
        markdown_parts.append("")

        for i, schema_info in enumerate(schemas_list):
            if not isinstance(schema_info, SchemaInfo):
                print(f"Warning: Encountered an unexpected item in schemas list: {type(schema_info)}")
                continue

            # Start of a schema item in the list
            schema_name_display = schema_info.full_name if schema_info.full_name else "Unnamed Schema"
            markdown_parts.append(f"## {schema_name_display}") # Main bullet point for schema name
                        
            description = f"**Description**: {schema_info.comment}" if schema_info.comment else ""
            markdown_parts.append(description)
            
            markdown_parts.append("") # Add a blank line for separation between schemas, or remove if too much space

    except Exception as e:
        error_message = f"Failed to retrieve schemas for catalog '{catalog_name}': {str(e)}"
        print(f"Error in get_catalog_summary: {error_message}")
        # Return a structured error message in Markdown
        return f"""# Error: Could Not Retrieve Catalog Summary
**Catalog:** `{catalog_name}`
**Problem:** An error occurred while attempting to fetch schema information.
**Details:**
```
{error_message}
```"""
    
    markdown_parts.append(f"**Total Schemas Found in `{catalog_name}`**: {schemas_found_count}")
    return "\n".join(markdown_parts)



def get_uc_all_catalogs_summary() -> str:
    """
    Fetches a summary of all available Unity Catalogs, including their names, comments, and types.
    Uses the global SDK client.
    """
    markdown_parts = ["# Available Unity Catalogs", ""]
    catalogs_found_count = 0

    try:
        print("Fetching all catalogs using global sdk_client...")
        catalogs_iterable = sdk_client.catalogs.list()
        catalogs_list = list(catalogs_iterable)

        if not catalogs_list:
            markdown_parts.append("- *No catalogs found or accessible.*")
            return "\n".join(markdown_parts)

        catalogs_found_count = len(catalogs_list)
        markdown_parts.append(f"Found {catalogs_found_count} catalog(s):")
        markdown_parts.append("")

        for catalog_info in catalogs_list:
            if not isinstance(catalog_info, CatalogInfo):
                print(f"Warning: Encountered an unexpected item in catalogs list: {type(catalog_info)}")
                continue
            
            markdown_parts.append(f"- **`{catalog_info.name}`**")
            description = catalog_info.comment if catalog_info.comment else "No description provided."
            markdown_parts.append(f"  - **Description**: {description}")
            
            catalog_type_str = "N/A"
            if catalog_info.catalog_type and hasattr(catalog_info.catalog_type, 'value'):
                catalog_type_str = catalog_info.catalog_type.value
            elif catalog_info.catalog_type: # Fallback if it's not an Enum but has a direct string representation
                catalog_type_str = str(catalog_info.catalog_type)
            markdown_parts.append(f"  - **Type**: `{catalog_type_str}`")
            
            markdown_parts.append("") # Add a blank line for separation

    except Exception as e:
        error_message = f"Failed to retrieve catalog list: {str(e)}"
        print(f"Error in get_uc_all_catalogs_summary: {error_message}")
        return f"""# Error: Could Not Retrieve Catalog List
**Problem:** An error occurred while attempting to fetch the list of catalogs.
**Details:**
```
{error_message}
```"""
    
    return "\n".join(markdown_parts)

