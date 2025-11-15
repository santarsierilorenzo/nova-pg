FROM python:3.10

# Create non-root user with bash
RUN useradd -ms /bin/bash myuser

# Use a neutral workdir (VS Code will override this later)
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER myuser
CMD ["bash"]
