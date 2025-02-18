FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

# copies everything from flows folder to the target folder
COPY flows /opt/prefect/flows 
RUN mkdir -p /opt/prefect/data/yellow