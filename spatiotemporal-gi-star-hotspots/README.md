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
spatiotemporal-gi-star-hotspots
- README.md
- src/
- HotzoneAnalysis.scala
- HotzoneUtils.scala
- HotcellAnalysis.scala
- HotcellUtils.scala

## Input Format
This project expects delimited text files containing:
- Pickup/event location as a point string (e.g., `x,y` or `(x,y)` depending on the source)
- Timestamp fields used to derive the temporal coordinate (Z)

> Datasets are not included due to size and licensing constraints. Provide the
> required input files locally.

## Notes
- Coordinates are treated as planar space for discretization and neighborhood computation.
- Empty cells are handled implicitly when computing global statistics.
- Output is hotspot-ranked grid cells based on Gi\* score.

