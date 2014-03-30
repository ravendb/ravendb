package net.ravendb.abstractions.spatial;

public class WktSanitizer {
  private String rectangleRegex ="^\\s*([+-]?(?:\\d+\\.?\\d*|\\d*\\.?\\d+))\\s*([+-]?(?:\\d+\\.?\\d*|\\d*\\.?\\d+))\\s*([+-]?(?:\\d+\\.?\\d*|\\d*\\.?\\d+))\\s*([+-]?(?:\\d+\\.?\\d*|\\d*\\.?\\d+))\\s*$";
  private String dimensionFlagRegex = "\\s+(?:z|m|zm|Z|M|ZM)\\s*\\(";
  private String reducerRegex = "([+-]?(?:\\d+\\.?\\d*|\\d*\\.?\\d+)\\s+[+-]?(?:\\d+\\.?\\d*|\\d*\\.?\\d+))(?:\\s+[+-]?(?:\\d+\\.?\\d*|\\d*\\.?\\d+))+";

  public String sanitize(String shapeWkt) {
    String result = shapeWkt;
    if (result.matches(rectangleRegex)) {
      return result;
    }

    result = shapeWkt.replaceAll(dimensionFlagRegex, " (");
    return  result.replaceAll(reducerRegex, "$1");
  }
}
