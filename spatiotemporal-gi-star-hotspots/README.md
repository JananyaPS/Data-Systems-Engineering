# Spatiotemporal Gi* Hotspot Detection (Apache Spark)

## Overview
This project implements **spatiotemporal hotspot detection at scale** using
Apache Spark. It converts raw event records into a space–time grid and computes
the **Getis-Ord Gi\*** statistic to identify statistically significant clusters
(hotspots) across both space and time.

The pipeline is designed to demonstrate scalable spatial analytics patterns:
grid aggregation, neighborhood construction, distributed joins, and statistical
scoring over large datasets.

## What This Project Does
- Discretizes geospatial points into grid cells using a configurable step size
- Aggregates event counts per cell over space and time
- Constructs each cell’s neighborhood (up to 27 neighbors in a 3×3×3 cube)
- Computes the Getis-Ord **Gi\*** score per cell to detect hotspots
- Returns hotspot-ranked cells (highest Gi\* first)

## Key Components
- **Hot Zone Analysis**: spatial containment checks (rectangle contains point) to
  count events within regions.
- **Hot Cell Analysis**: spatiotemporal cell modeling + Gi\* scoring for hotspot detection.

## Tech Stack
- Apache Spark
- SparkSQL
- Scala
- Spatial/Spatiotemporal Analytics
- Statistical Scoring (Getis-Ord Gi\*)

## Project Structure
