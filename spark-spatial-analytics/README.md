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
spark-spatial-analytics/
  - README.md
  - src/
  - SpatialQuery.scala

## Input Format
This project expects delimited text/CSV files where each record contains a geometry string:

- **Point format**: `x,y`
- **Rectangle format**: `x1,y1,x2,y2`

> Datasets are not included due to size/licensing constraints. Provide the required
> input files locally and update run commands accordingly.

## How It Works (High Level)
1. Load input datasets into Spark DataFrames
2. Register spatial UDFs (containment and distance)
3. Execute SparkSQL queries for each operation type
4. Output result counts (and optionally persist output, depending on your runner)

## Notes
This project emphasizes distributed query processing patterns such as joins,
filters, and UDF evaluation, key building blocks for scalable geospatial analytics.
