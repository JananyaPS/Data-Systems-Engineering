# Spark Spatial Analytics (SparkSQL + Scala)

## Overview
This project implements **distributed spatial query processing** using Apache
Spark and SparkSQL. It focuses on scalable execution of common spatial
operations (range and distance queries) over large geospatial datasets using
custom UDFs and Spark’s parallel execution engine.

The goal is to demonstrate how spatial analytics can be implemented efficiently
in a big-data environment without relying on single-node GIS tooling.

## Features
- **Range Query**: retrieve points that fall within a rectangular boundary
- **Range Join Query**: join points with spatial regions that contain them
- **Distance Query**: find points within a specified distance from a reference point
- **Distance Join Query**: join two point datasets based on a distance threshold
- **Custom Spatial UDFs**: geometric containment and distance checks implemented in Scala

## Tech Stack
- Apache Spark
- SparkSQL
- Scala
- UDFs (user-defined functions)

## Project Structure
