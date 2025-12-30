package spatial.analytics

object SpatialZoneFunctions {

  /**
   * Returns true if the point lies inside (or on the boundary of) the rectangle.
   *
   * @param queryRectangle "x1,y1,x2,y2"
   * @param pointString    "x,y" (parentheses optional if cleaned before)
   */
  def contains(queryRectangle: String, pointString: String): Boolean = {
    if (queryRectangle == null || queryRectangle.isEmpty || pointString == null || pointString.isEmpty) return false

    try {
      val rect = queryRectangle.split(",")
      val pt = pointString.split(",")

      if (rect.length != 4 || pt.length != 2) return false

      val x1 = rect(0).trim.toDouble
      val y1 = rect(1).trim.toDouble
      val x2 = rect(2).trim.toDouble
      val y2 = rect(3).trim.toDouble

      val px = pt(0).trim.toDouble
      val py = pt(1).trim.toDouble

      val minX = math.min(x1, x2)
      val maxX = math.max(x1, x2)
      val minY = math.min(y1, y2)
      val maxY = math.max(y1, y2)

      px >= minX && px <= maxX && py >= minY && py <= maxY
    } catch {
      case _: NumberFormatException => false
      case _: ArrayIndexOutOfBoundsException => false
    }
  }
}
