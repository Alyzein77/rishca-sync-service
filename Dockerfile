FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create directories for file storage
RUN mkdir -p /app/files/templates /app/files/output

# Bake template files into the image so they survive redeploys
COPY templates/Team_Budget_Clean.xlsx /app/files/templates/Team_Budget_Clean.xlsx
COPY templates/Rishca_OS_Financial_Model.xlsx /app/files/templates/Rishca_OS_Financial_Model.xlsx
COPY templates/Rishca_OS_Financial_Pitch_Slides.pptx /app/files/templates/Rishca_OS_Financial_Pitch_Slides.pptx

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
