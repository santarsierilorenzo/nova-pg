FROM python:3.10

RUN useradd -m myuser

WORKDIR /home/nova

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt || true

COPY . .
RUN pip install --no-cache-dir -e .

USER myuser
ENV PYTHONPATH="/home/nova/"

CMD ["bash"]
