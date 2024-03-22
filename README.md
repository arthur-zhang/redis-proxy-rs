# Redis proxy in rust

## Introduction

This project presents a Redis proxy implemented in Rust. It is specifically tailored for use in multi-cloud architectures, providing a seamless Redis service across various cloud platforms.

## Features

- **Redis Protocol Support**: Fully compatible with Redis protocol up to version 7.x, ensuring broad compatibility with existing Redis clients and tools.
- **Key-Based Filter Chain**: Implements a flexible filter chain based on keys, allowing for fine-grained control over request handling.
- **Synchronous Double Writing**: Ensures data consistency and durability by writing data synchronously to multiple instances.
- **High-Performance Key Matching**: Utilizes a trie-based structure for efficient key matching, significantly improving performance for large datasets.
- **Chunked Response Delivery**: Streams responses in chunks to clients, optimizing network usage and improving response times for large data retrievals.
- **Rich Extensible Filters**: Offers a wide range of extensible filters, including blacklist based on keys and commands, logging, rate limiting, and more, providing enhanced control and security.
- **Etcd Based Real-Time Filter Configuration Updates**: Supports real-time updates and distribution of filter configurations using etcd, ensuring dynamic adaptability and responsiveness to changing requirements.

## Usage

```shell
redis_proxy_server ./config.toml
```