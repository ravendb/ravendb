package net.ravendb.abstractions.spatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;


/**
 * Converts shape objects to strings, if they are not already a string
 */
public class ShapeConverter {


  private final static GeoJsonWktConverter geoJsonConverter = new GeoJsonWktConverter();
  private final static String REG_EXP_X = "^(?:x|longitude|lng|lon|long)$";
  private final static String REG_EXP_Y = "^(?:y|latitude|lat)$";

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
    if (value instanceof Iterable) {
      List<Object> objects = new ArrayList<>();

      Iterator<?> iterator = ((Iterable<?>) value).iterator();
      while (iterator.hasNext()) {
        objects.add(iterator.next());
      }

      boolean allNumbers = true;
      for (Object o : objects) {
        allNumbers &= isNumber(o);
      }

      if (objects.size() > 1 && allNumbers) {
        resultRef.value = makePoint(getDouble(objects.get(0)), getDouble(objects.get(1)));
        return true;
      }

      Map<String, Object> keyValues = new HashMap<>();
      // try to fill using ravenJObject
      for (Object o : objects) {
        if (o instanceof Entry) {
          Entry<?, ?> e = (Entry<?, ?>) o;
          if (e.getKey() instanceof String && e.getValue() instanceof RavenJToken && isNumber(e.getValue())) {
            keyValues.put((String)e.getKey(), e.getValue());
          }
        }
      }

      if (keyValues.size() > 0) {
        String x1 = findKeyThatMatches(keyValues, REG_EXP_X);
        String y1 = findKeyThatMatches(keyValues, REG_EXP_Y);
        if (x1 != null && y1 != null) {
          resultRef.value = makePoint(getDouble(keyValues.get(x1)), getDouble(keyValues.get(y1)));
          return true;
        }
      }

    }

    if (value instanceof RavenJObject) {
      RavenJObject ravenValue = (RavenJObject) value;
      if (geoJsonConverter.tryConvert(ravenValue, resultRef)) {
        return true;
      }
    }

    resultRef.value = null;
    return false;
  }

  private String findKeyThatMatches(Map<String, Object> keyValues, String regExp) {
    for (String key: keyValues.keySet()) {
      if (key.toLowerCase().matches(regExp)) {
        return key;
      }
    }
    return null;
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
    return String.format(Constants.getDefaultLocale(), "POINT (%f %f)", x, y);
  }


}
