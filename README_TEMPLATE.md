<a name="readme-top"></a>

<!-- You will have to swap edgetech-template w/ your repo's name-->

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

<!-- Add any relevant tools -->

[Python]: https://img.shields.io/badge/python-000000?style=for-the-badge&logo=python
[Python-url]: https://www.python.org
[Poetry]: https://img.shields.io/badge/poetry-20232A?style=for-the-badge&logo=poetry
[Poetry-url]: https://python-poetry.org
[Docker]: https://img.shields.io/badge/docker-35495E?style=for-the-badge&logo=docker
[Docker-url]: https://www.docker.com

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

<!-- Substitute this with the name of your project EdgeTech-* -->
<!-- Replace edgetech-template in the hyperlinks w. your project -->

<h1 align="center">EdgeTech-Template</h1>

  <p align="center">
   TODO: Some information about your project here... 
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

<!-- Add any additional tools you added -->

[![Python][Python]][Python-url]
[![Poetry][Poetry]][Poetry-url]
[![Docker][Docker]][Docker-url]

## Getting Started

To run this repo, simply run:

```
docker-compose up
```

The audio recorder is containerized and dependencies are managed using [poetry]("https://python-poetry.org"). 

### Prerequisites

Running this repo requires that you have [Docker](https://www.docker.com) installed. 

## Usage

<!-- This should be the case for all edgetech modules -->

Spinning up this system requires an MQTT server and this container to be included in your `docker-compose.yml`. You can find an example of this workflow in this repository's `docker-compose.yml`. Additionally, some editing of relevant environment variables will be required based upon your system's configuration of topics to subscribe to and MQTT configuration. Examples of these environment variables can be found in this repository's `.env` file. 

Copying the project `docker-compose` statements into a master `docker-compose.yml` and  `.env` files with your entire system of containers is the preferred workflow. Find an application architecture diagram example of how the usage of this module was envisioned below.

<!-- Use the Labs Brand Colors in Mermaid Diagrams: #80c342,  #181245,  #f9d308,  #0072bc, #f05343, #65cbe7, #6657d3, #5f6475, #ffee97, #d0d2d9 -->


```mermaid 

flowchart TD
    A(A) -- A Description of the Link --> B{B}
    C{C} -- A Description of the Link --> A(A)

style A fill:#0072bc,color:#ffffff
style B fill:#80c342,color:#ffffff
style C fill:#f05343,color:#ffffff

```

## Roadmap

- TBA

<!-- replace edgetech-template w/ your repo's name -->

See the [open issues](https://github.com/IQTLabs/edgetech-template/issues) for a full list of proposed features (and known issues).

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

Distributed under the [Apache 2.0](https://github.com/IQTLabs/edgetech-audio-recorder/blob/main/LICENSE). See `LICENSE.txt` for more information.

## Contact IQTLabs

  - Twitter: [@iqtlabs](https://twitter.com/iqtlabs)
  - Email: info@iqtlabs.org

See our other projects: [https://github.com/IQTLabs/](https://github.com/IQTLabs/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>