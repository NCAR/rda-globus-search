# Dataset Search

This application is a command-line tool that builds an index of metadata to
enable search and discovery of datasets archived in the NSF NCAR Geoscience
Data Exchange (GDEX).  The metadata is extracted from the GDEX database,
then assembled and ingested into a Globus Search index, which can then be
used in the web app for dataset search and discovery.

This application is adapted from the [searchable-files-demo application](https://github.com/globus/searchable-files-demo) available from the Globus public github.

## Architecture

The app is broken up into five main components:

- the **Extractor** (`src/gdex_globus_search/extractor.py`)

Extracts metadata from the database, and formats that data into JSON
files.

- the **Assembler** (`src/gdex_globus_search/assembler.py`)

Combines the output of the Extractor to produce ingest documents for Globus 
Search. An ingest document is data formatted for submission to Globus Search, 
containing searchable data and visibility information for who is allowed to 
search on and view different parts of the data.

- the **Submitter** (`src/gdex_globus_search/submit.py`)

The Submitter sends ingest documents to the Globus Search service.

- the **Watcher** (`src/gdex_globus_search/watcher.py`)

The Watcher monitors ingest tasks in Globus Search and waits for completion or failure.

- the **Ingester** (`src/gdex_globus_search/ingester.py`)

The Ingester runs the full extract, assemble, submit, and watch pipeline in sequence
with a single command line call.  This is equivalent to running each subcommand 
(`extract`, `assemble`, `submit`, and `watch`) individually.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to 
install `gdex_globus_search`.

From within your Python virtual environment:
```
pip install git+https://github.com/NCAR/gdex-globus-search
```

After installation, the cli command `dataset-search` will be available in
the /bin directory of your virtual environment.

### Running the Workflow

Each component of the Dataset Search app is run with a separate
subcommand. Each supports a `--help` option for full details on its
usage.
```
dataset-search extract --help
dataset-search assemble --help
dataset-search submit --help
dataset-search watch --help
```
The order of these commands matters, as each command's output is the input to
the next command.

The entire workflow can run with the single command 
```
dataset-search ingest --dsid DATASET_ID
```
Alternatively, the entire workflow can be run in one line by simply running each command
back-to-back:
```
dataset-search extract && dataset-search assemble && dataset-search submit && dataset-search watch
```
Note that the `extract` and `ingest` subcommands require a dataset ID (format 'dnnnnnn') as 
input via the command line option `--dsid`.

### Example usage
```
$ dataset-search extract --dsid d731000 --output /path/to/extracted/json/output

metadata extraction complete
results visible in
  /path/to/extracted/json/output

$ dataset-search assemble

ingest document assembly complete
results visible in
  /path/to/assembled/globus/search/output

$ dataset-search submit

ingest document submission (task submission) complete
task IDs are visible in
    /path/to/task/submit/ingest-tasks.txt

$ dataset-search watch

  [####################################]  100%
Tasks all completed successfully (1/1)
```

### Importing cli subcommands within a Python interpreter or script
The cli subcommands can be imported into a Python interpreter or script in lieu
of calling them from the command line.  These functions are 
[`click.command`](https://click.palletsprojects.com/en/stable/api/#decorators) 
objects that expect a `sys.argv` list as input.  

For example, the subcommand `dataset-search extract` is defined as the
function `gdex_globus_search.extractor.extract()`, and expects the required
option `--dsid` and optional parameters `--output` and `--clean`.  The 
`extract` subcommand can therefore be imported and called inside a Python
interpreter or script as follows:
```
from gdex_globus_search.extractor import extract
args = ["--dsid", "d731000", "--output", "/path/to/json/output"]
extract(args, standalone_mode=False)
```
Note that the above example specifies 
[`standalone_mode=False`](https://click.palletsprojects.com/en/stable/api/#click.BaseCommand.main), 
otherwise Python `click` will exit the interpreter or script after calling the
function.

### Querying Results

The Searchable Files demo app includes a query command which you can use to
search dataset metadata. Search results will be output in the JSON format produced by
the Globus Search service.  This is a simple query service meant only to verify
a few selected metadata parameters were ingested correctly, and should not be
treated as a fully supported query service.

For advanced queries, include the `--advanced` option flag and use advanced
query syntax in the query string.  See [Query Syntax](https://docs.globus.org/api/search/query/#query_syntax)
in the Globus Search documentation for more information.

See
```
dataset-search query --help
```
for more details.

A simple text query will search across all metadata fields for matches.
For example:
```
dataset-search query "precipitation"
```
will submit a query which matches `"precipitation"`.

#### Dumping the Query

If you want to inspect the query which the `dataset-search` command
is generating instead of submitting the query, you can use
`--dump-query` to write the query to standard out, as in
```
dataset-search query "NCEP" --variables=temperature --dump-query
```
## Resources

This app uses the 
[`SearchClient` class from the Globus SDK](https://globus-sdk-python.readthedocs.io/en/stable/services/search.html).

The full [Globus Search documentation](https://docs.globus.org/api/search/) offers full
details about the service and reference documentation for all of
its supported methods and features.
