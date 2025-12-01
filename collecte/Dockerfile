FROM python:3.12-slim

WORKDIR /app

# Copie et installe les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie tout le code
COPY . .

# Garde le conteneur vivant
CMD ["tail", "-f", "/dev/null"]