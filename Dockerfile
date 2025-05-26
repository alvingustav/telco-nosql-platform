FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r Requirements.txt

# Set environment variable for Flask (opsional, tapi berguna untuk dev)
ENV FLASK_APP=src.web_app.app:app
ENV FLASK_ENV=production

# Jalankan Flask via module (hindari "ModuleNotFoundError")
CMD ["python", "-m", "src.web_app.app"]
