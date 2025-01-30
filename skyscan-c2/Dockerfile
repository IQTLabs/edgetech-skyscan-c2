FROM iqtlabs/edgetech-core:latest
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean -y
COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.5.1
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
ADD ./ACFTREF.txt.zst .
ADD MASTER.txt.zst .
ADD ./c2_pub_sub.py .

RUN wget https://github.com/IQTLabs/edgetech-axis-ptz-controller/raw/main/axis-ptz-controller/axis_ptz_utilities.py

ENTRYPOINT [ "python3", "c2_pub_sub.py" ]