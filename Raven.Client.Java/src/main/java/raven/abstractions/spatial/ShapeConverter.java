package raven.abstractions.spatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import raven.abstractions.basic.Reference;
import raven.abstractions.json.linq.JTokenType;
import raven.abstractions.json.linq.RavenJValue;

/**
 * Converts shape objects to strings, if they are not already a string
 */
public class ShapeConverter {


  private final static GeoJsonWktConverter geoJsonConverter = new GeoJsonWktConverter();
  //TODO: regexp for x and y

  @SuppressWarnings("unchecked")
  public boolean tryConvert(Object value, Reference<String> resultRef) {
    if (value instanceof String) {
      resultRef.value = (String) value;
      return true;
    }

    if (value instanceof RavenJValue) {
      RavenJValue jValue = (RavenJValue) value;
      if (jValue.getType() == JTokenType.STRING) {
        resultRef.value = (String) jValue.getValue();
        return true;
      }
    }

    if (value.getClass().isArray()) {
      value = Arrays.asList((Object[])value);
    }
    if (value instanceof Collection) {
      List<Object> list = new ArrayList<>((Collection<Object>)value);
      boolean allNumbers = true;
      for (int i =0 ;i < list.size(); i++) {
        if (!isNumber(list.get(i))) {
          allNumbers = false;
          break;
        }
      }
      if (list.size() > 1 && allNumbers) {
        resultRef.value = makePoint(getDouble(list.get(0)), getDouble(list.get(1)));
        return true;
      }

      //TODO: other types
    }

    resultRef.value = null;
    return false;
  }

  private boolean isNumber(Object obj) {
    if (obj instanceof Number) {
      return true;
    }
    if (obj instanceof RavenJValue) {
      RavenJValue rValue = (RavenJValue) obj;
      return rValue.getType() == JTokenType.FLOAT || rValue.getType() == JTokenType.INTEGER;
    }
    return false;
  }

  private double getDouble(Object obj) {
    if (obj instanceof Number) {
      return ((Number)obj).doubleValue();
    }
    if (obj instanceof RavenJValue)  {
      RavenJValue rValue = (RavenJValue) obj;
      if (rValue.getType() == JTokenType.FLOAT || rValue.getType() == JTokenType.INTEGER) {
        return ((Number)rValue.getValue()).doubleValue();
      }
    }

    return 0.0;
  }

  protected String makePoint(double x, double y) {
    return String.format("POINT (%f %f)", x, y);
  }


}
