# Overview Python Examples

There are two examples
1. PyAthena
2. aws-data-wrangler

They both have differences in how to query.

In addition there is a an example of aws-data-wrangler saving directly to the glue catalog, this is useful when you have the data is a dataframe and you have the the backend sources setup for a blue/green on/off. You do not need a crawler to crawler the bucket and you can switch the backend data source from blue/green during the save to not have to deal with blue/green buckets. Instead you database would always point to the latest color, but the data behind the scenes would reside within the blue or green bucket.

# Python Pandas Athena Example

From [PyAthena](https://pypi.org/project/PyAthena/) documentation, don't forget to set the environment variables as appropriate for `aws_access_key_id_athena` and `aws_secret_access_key`.

# Python AWS Data Wrangler Athena Example

Either set the `aws_access_key_id_athena` and `aws_secret_access_key` and update the code or use the `profile` variable, currently set to default.

## How-To

1. Install a virtual environment

```bash
python3 -m venv venv
```

2. Activate virtual environment
* run `sh dev-setup.sh`
* 
```bash
#Mac
source ./venv/bin/activate
#Windows
source .\scripts\activate
```

3. Install the pip requirements.

```bash
pip install -r requirements.txt
```

4. Update the variables in the script you want to use. `s3_staging_dir`, `database`, `table_name`, etc.

5. Run
* vscode
* 
```bash
python pyathena/example.py
```


### Formatting 

Python formatting can be done by two well known options, 
1. black
2. yapf - used in the project

For import formating:
1. isort - globally install
