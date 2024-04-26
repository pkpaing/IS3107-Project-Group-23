# Use the official Airflow image as a base
FROM apache/airflow:2.8.1

USER root
# Install any necessary system packages
RUN apt-get update && apt-get install -y \
    libpq-dev \
    build-essential \
    libjpeg-dev \
    zlib1g-dev \
    libfreetype6-dev \
    liblcms2-dev \
    libopenjp2-7 \
    libtiff-dev \
    libxcb1 \
    libzip-dev \
    # Dependencies for Google API and other integrations
    default-libmysqlclient-dev \
    gcc \
    libc-dev \
    # Clean up the apt cache to reduce image size
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Install the required Python libraries
RUN pip install --no-cache-dir \
    pandas==1.2.3 \
    requests==2.25.1 \
    scikit-learn==0.24.1 \
    numpy==1.20.1 \
    xlrd==2.0.1 \
    Pillow==8.1.0 \
    google-auth==1.27.0 \
    google-api-python-client==1.12.8 \
    zipfile36==0.1.3 \
    # Google service account for API access
    google-auth-httplib2==0.0.4 \
    google-auth-oauthlib==0.4.3

# Set the working directory to the home directory of the airflow user
WORKDIR /home/airflow
