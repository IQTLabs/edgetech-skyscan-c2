<a name="readme-top"></a>

[contributors-shield]: https://img.shields.io/github/contributors/IQTLabs/edgetech-template.svg?style=for-the-badge
[contributors-url]: https://github.com/IQTLabs/edgetech-template/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/IQTLabs/edgetech-template.svg?style=for-the-badge
[forks-url]: https://github.com/IQTLabs/edgetech-template/network/members
[stars-shield]: https://img.shields.io/github/stars/IQTLabs/edgetech-template.svg?style=for-the-badge
[stars-url]: https://github.com/IQTLabs/edgetech-template/stargazers
[issues-shield]: https://img.shields.io/github/issues/IQTLabs/edgetech-template.svg?style=for-the-badge
[issues-url]: https://github.com/IQTLabs/edgetech-template/issues
[license-shield]: https://img.shields.io/github/license/IQTLabs/edgetech-template.svg?style=for-the-badge
[license-url]: https://github.com/IQTLabs/edgetech-template/blob/master/LICENSE.txt
[product-screenshot]: images/screenshot.png
[python]: https://img.shields.io/badge/python-000000?style=for-the-badge&logo=python
[python-url]: https://www.python.org
[poetry]: https://img.shields.io/badge/poetry-20232A?style=for-the-badge&logo=poetry
[poetry-url]: https://python-poetry.org
[docker]: https://img.shields.io/badge/docker-35495E?style=for-the-badge&logo=docker
[docker-url]: https://www.docker.com

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

<br />
<div align="center">
  <a href="https://iqtlabs.org/">
    <img src="images/logo.png" alt="Logo" width="331" height="153">
  </a>

<h1 align="center">EdgeTech-Template</h1>

  <p align="center">
    This repo is designed to be part of a SkyScan system. SkyScan automatically points a Pan Tilt Zoom (PTZ) camera at an aircraft based on the location information broadcast in an ADS-B message. SkyScan C2 ingests a ledger of potentional aircraft and selects one to point the camera at. It is run as a Docker container and messages are passed to it using MQTT. 
    <br/>
    <br/>
    <h3>Configuration</h3>
    SkyScan C2 makes it selection based on the distance of aircraft from the camera location. Based on where the camera is located and weather conditions, it may not be able to see all of the surrounding aircrarft. You can have SkyScan C2 ignore these obscured aircraft by configuring the following environment variables:
    - **MIN_TILT**: The minimum tilt angle above the horizon for the camera. This is useful for when trees or buildings might obscure the horizon
    - **MIN_ALTITUDE**: The minimum altitude of an aircraft. This is useful when there are nearby aircraft that are on the ground and should be ignored.
    - **MAX_ALTITUDE**: The maximum altitude of an aircraft. This is useful when there are clouds and you want to ignore any aircrafts that are in the clouds.
    <br/>
    <br/>
    <h3>Docker Compose</h3>
    SkyScan is designed to be stood up as a series of Docker containers. Docker Compose makes it easy to do coordinate all of the containers. The <a href="https://github.com/IQTLabs/edgetech-skyscan">EdgeTech SkyScan</a> repo provides an example of a `docker-compose.yaml` file that can be used to startup an instance. The `docker-compose.yaml` file include in this repo provides a minimal example of how to start and configure a SkyScan C2 container. You can use this Docker Compose file as a starting point if you wish to include the SkyScan C2 container in a custom system.
    <br/>
    <br/>
    <h3>Environment Files</h3>
    Environment files are used to capture the configuration of SkyScan C2. 
    <br/>
    <br/>
    <a href="https://github.com/IQTLabs/edgetech-template/pulls">Make Contribution</a>
    ·
    <a href="https://github.com/IQTLabs/edgetech-template/issues">Report Bug</a>
    ·
    <a href="https://github.com/IQTLabs/edgetech-template/issues">Request Feature</a>
  </p>
</div>

### Built With

[![Python][python]][python-url]
[![Poetry][poetry]][poetry-url]
[![Docker][docker]][docker-url]

### Modules Built Based on this Template

<p align="left">
- <a href="https://github.com/IQTLabs/edgetech-daisy">edgetech-daisy</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-filesaver">edgetech-filesaver</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-audio-recorder">edgetech-audio-recorder</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-c2">edgetech-c2</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-telemetry-pinephone">edgetech-telemetry-pinephone</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-s3-uploader">edgetech-s3-uploader</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-couchdb-startup">edgetech-couchdb-startup</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-couchdb-saver">edgetech-couchdb-saver</a>
<br/>
- <a href="https://github.com/IQTLabs/edgetech-http-uploader">edgetech-http-uploader</a>
<br/>
</p>

### Projects Built Using the EdgeTech Framework

<p align="left">
- <a href="https://github.com/IQTLabs/aisonobuoy-collector-pinephone">aisonobuoy-collector-pinephone</a>
</p>

### Prerequisites

Running this repo requires that you have [Docker](https://www.docker.com) for containerization, [Poetry][poetry-url] for dependency management, and [Python 3.11.1][python-url] is the version we've been building with.

## Usage

Hit `Use this template` and `Create a new repository` to get started. Name it `edgetech-` plus whatever functionality you're adding. You'll also want to rename everything that says `template` in this repository with that name.

Please use `README_TEMPLATE.md` as a template for your own `README.md` file. You'll want to delete this `README.md` and rename the `README_TEMPLATE.md` to `README.md`.

Within the `template` directory, you should find several files: a `Dockerfile`, `pyproject.toml`, `poetry.lock`, and `template_pub_sub.py`. These files are the core of the template and are required to build your own module, though you'll want to rename the `template_pub_sub.py` file to whatever your module is called.

`pyproject.toml` is generated by running `poetry init` as we recommend using [`poetry`][poetry-url] to manage dependencies.

Once the `pyproject.toml` has been created, use `poetry install` to generate the `poetry.lock` file. You'll want to run `poetry config virtualenvs.create false` and `poetry install --no-dev` before calling `poetry install`.

### BaseMQTTPubSub Child Class

The core module that is a python wrapper around interacting with MQTT system, heartbeats and tests. The `template_pub_sub.py` file includes examples of recommended usage of the `BaseMQTTPubSub` module and how to build a child class using it.

An outline of the basic functionality can be found below.

Inheriting `BaseMQTTPubSub`:

```python
from base_mqtt_pub_sub import BaseMQTTPubSub

class TemplatePubSub(BaseMQTTPubSub):
    def __init__(
        self: Any,
        ...
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
```

The use of `**kwargs` allows you to override any of the class parameters set in the `BaseMQTTPubSub` constructor.

In the constructor of the child class, it is recommended that you connect to the MQTT client and publish a message to the `/registration` topic upon successful connection.

```python
self.connect_client()
sleep(1)
self.publish_registration("Template Module Registration")
```

Every child class should include a `main()` function which includes a publishing to the `/heartbeat` channel to keep the connection alive and any subscriptions to other topics in the system. It should also include a `while True` loop to keep the main thread alive and flush all scheduled function calls.

```python
  def main(self: Any) -> None:
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="Template Module Heartbeat"
        )

        self.add_subscribe_topic(self.example_topic, self._example_callback)

        ...

        while True:
            try:
                schedule.run_pending()
                sleep(0.001)

            except Exception as e:
                if self.debug:
                    print(e)
```

To call the child class see that the environment variables are passed via `docker-compose` and passed to the constructor. The `main()` function is then called.

```python
    template = TemplatePubSub(
        ...,
        mqtt_ip=os.environ.get("MQTT_IP"),
    )
    template.main()
```

### Docker

Examples of a `Dockerfile` and `docker-compose.yaml` can also be found in this repo. Adding whatever environment variables that your class needs should go into your `.env` file after renaming the `template.env` and paths/names will need to be adjusted as well. The `Dockerfile` should only require script name/path changes as well.

### Topic Names

Recommended topic names should follow the format specified below.

```python
f"{DEVICE}/{HOST_NAME}/{DATA_TYPE}/{CONTAINER_NAME}/{TYPE_LITERAL}"
```

Example:

```python
f"/AISonobuoy/{HOST_NAME}/AIS/edgetech-daisy/bytestring
```

## Roadmap

- how to write tests for a child class of the core module

See the [open issues](https://github.com/github_username/repo_name/issues) for a full list of proposed features (and known issues).

## Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b dev`)
3. Commit your Changes (`git commit -m 'adding some feature'`)
4. Run (and make sure they pass):

```
black --diff --check *.py

pylint --disable=all --enable=unused-import *.py

mypy --allow-untyped-decorators --ignore-missing-imports --no-warn-return-any --strict --allow-subclassing-any *.py
```

If you do not have them installed, you can install them with `pip install "black<23" pylint==v3.0.0a3 mypy==v0.991`.

5. Push to the Branch (`git push origin dev`)
6. Open a Pull Request

See `CONTRIBUTING.md` for more information.

## License

Distributed under the [Apache 2.0](https://github.com/IQTLabs/edgetech-template/blob/main/LICENSE). See `LICENSE.txt` for more information.

## Contact IQTLabs

- Twitter: [@iqtlabs](https://twitter.com/iqtlabs)
- Email: info@iqtlabs.org

See our other projects: [https://github.com/IQTLabs/](https://github.com/IQTLabs/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>
