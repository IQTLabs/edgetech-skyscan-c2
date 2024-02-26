FROM iqtlabs/edgetech-core:latest
COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.5.1
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
ADD ./c2_pub_sub.py .
ADD ./axis_ptz_utilities.py .
ENTRYPOINT [ "python3", "c2_pub_sub.py" ]