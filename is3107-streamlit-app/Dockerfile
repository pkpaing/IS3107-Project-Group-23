# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file to the container
COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . /app

# Expose the port that Streamlit listens on
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app.py"]