[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) 

# Geode Command Region Pattern
1. [Overview](#overview)
2. [Usage](#usage)

## <a name="overview"></a>Overview

[Apache Geode](http://geode.apache.org/) is a data management platform that provides real-time, 
consistent access to data-intensive applications throughout widely distributed cloud architectures.

This project simply demonstrates the usage of the Command Region Pattern in order to distribute
units of work across different clusters. It can be used to test your own distributed commands through
the `geode-dunit` module.

## <a name="usage"></a>Usage

- Check out this repository.
- Import the project into your favourite IDE.
- Add your custom implementations of the `DistributedCommand` class.
- Test your implementation in a multi-site deployments by extending the `AbstractDistributedCommandTest` class.
