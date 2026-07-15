FROM python:3.12-slim

WORKDIR /app

# The shared package (food_delivery_shared) carries the pure domain logic
# (load_logic) that generator.py imports. Installing the wheel keeps the
# generator and the tested logic in sync — same code, same source of truth.
COPY food_delivery_shared-*.whl .
RUN pip install --no-cache-dir psycopg2-binary==2.9.9 food_delivery_shared-*.whl

COPY generator.py .

CMD ["python", "-u", "generator.py"]