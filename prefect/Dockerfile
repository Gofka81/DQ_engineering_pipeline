FROM prefecthq/prefect:3-python3.11

WORKDIR /prefect

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy flows
COPY . /prefect
