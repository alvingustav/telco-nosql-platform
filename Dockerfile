FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Salin semua file project
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r Requirements.txt

# Default command
CMD ["python", "src/web_app/app.py"]
