# Stage 1: Build the React frontend
FROM node:20-alpine AS frontend-builder
WORKDIR /app/frontend

# Copy package files and install dependencies
COPY frontend/package*.json ./
RUN npm install

# Copy frontend source code and build
COPY frontend/ ./
RUN npm run build

# Stage 2: Setup the Python backend
FROM python:3.11-slim
WORKDIR /app

# Set environment variables for production
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production
ENV PORT=8080

# Install system dependencies (required for ML packages and PostgreSQL)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY backend/requirements.txt ./backend/
RUN pip install --no-cache-dir -r backend/requirements.txt
RUN pip install gunicorn

# Copy backend source code
COPY backend/ ./backend/

# Copy built frontend assets from Stage 1
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

# Expose the standard web port
EXPOSE 8080

# Start Gunicorn server from the backend directory
# Pointing to the factory function create_app()
CMD ["gunicorn", "--chdir", "backend", "app:create_app()", "-b", "0.0.0.0:8080", "--workers", "2", "--timeout", "120"]
