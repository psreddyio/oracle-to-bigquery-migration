FROM python:3.8-slim

# Set the working directory 
WORKDIR /app

# Copy everything
COPY . /app

# Read and set environment variables
ENV oracledb_username ################
ENV oracledb_hostname ################
ENV oracledb_password ################
ENV oracledb_port ################
ENV oracledb_sid ################

# Install dependencies
RUN pip install --index-url https://pypi.org/simple --no-cache-dir --upgrade pip \
    && pip install --index-url https://pypi.org/simple --no-cache-dir -r requirements.txt   

# Run the python script
CMD ["python", "app.py"]
# EXPOSE 3000
