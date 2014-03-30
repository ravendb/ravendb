package net.ravendb.abstractions.spatial;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;


public class GeoJsonWktConverter {

  public boolean tryConvert(RavenJObject json, Reference<String> result) {
    StringBuilder builder = new StringBuilder();
    Reference<StringBuilder> builderRef = new Reference<>(builder);
    if (tryParseGeometry(json, builderRef)) {
      result.value = builder.toString();
      return true;
    }
    if (tryParseFeature(json, builderRef)) {
      result.value = builder.toString();
      return true;
    }
    result.value = "";
    return false;
  }

  @SuppressWarnings("null")
  private boolean tryParseTypeString(RavenJObject obj, Reference<String> result) {
    Reference<RavenJToken> typeRef = new Reference<>();
    if (obj != null) {
      if (!obj.tryGetValue("type", typeRef)) {
        return false;
      }
    }
    RavenJValue value = (RavenJValue) typeRef.value;
    result.value = value.value(String.class);

    return typeRef != null;
  }


  private boolean tryParseFeature(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<String> typeStringRef = new Reference<>();
    if (tryParseTypeString(obj, typeStringRef) && typeStringRef.value.equalsIgnoreCase("feature")) {
      Reference<RavenJToken> geometryRef = new Reference<>();
      if (obj.tryGetValue("geometry", geometryRef) && tryParseGeometry((RavenJObject) geometryRef.value, builder)) {
        return true;
      }
    }
    return false;
  }

  private boolean tryParseGeometry(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<String> typeStringRef = new Reference<>();
    if (!tryParseTypeString(obj, typeStringRef)) {
      return false;
    }

    typeStringRef.value = typeStringRef.value.toLowerCase();

    String typeString = typeStringRef.value;

    switch (typeString) {
    case "point":
      return tryParsePoint(obj, builder);
    case "linestring":
      return tryParseLineString(obj, builder);
    case "polygon":
      return tryParsePolygon(obj, builder);
    case "multipoint":
      return tryParseMultiPoint(obj, builder);
    case "multilinestring":
      return tryParseMultiLineString(obj, builder);
    case "multipolygon":
      return tryParseMultiPolygon(obj, builder);
    case "geometrycollection":
      return tryParseGeometryCollection(obj, builder);
    default:
      return false;
    }
  }

  private boolean tryParsePoint(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<RavenJToken> coordRef = new Reference<>();
    if (obj.tryGetValue("coordinates", coordRef)) {
      if (!(coordRef.value instanceof RavenJArray)) {
        return false;
      }
      RavenJArray coordinates = (RavenJArray) coordRef.value;

      if (coordinates == null || coordinates.size() < 2) {
        return false;
      }

      builder.value.append("POINT (");
      if (tryParseCoordinate(coordinates, builder)) {
        builder.value.append(")");
        return true;
      }
    }
    return false;
  }

  private boolean tryParseLineString(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<RavenJToken> coordRef = new Reference<>();
    if (obj.tryGetValue("coordinates", coordRef)) {
      RavenJArray coordinates = (RavenJArray) coordRef.value;
      builder.value.append("LINESTRING (");
      if (coordinates != null && tryParseCoordinateArray(coordinates, builder)) {
        builder.value.append(")");
        return true;
      }
    }
    return false;
  }

  private boolean tryParsePolygon(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<RavenJToken> coordRef = new Reference<>();
    if (obj.tryGetValue("coordinates", coordRef)) {
      RavenJArray coordinates = (RavenJArray) coordRef.value;

      builder.value.append("POLYGON (");
      if (coordinates != null && coordinates.size() > 0 && tryParseCoordinateArrayArray(coordinates, builder))
      {
        builder.value.append(")");
        return true;
      }
    }
    return false;
  }

  private boolean tryParseMultiPoint(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<RavenJToken> coordRef = new Reference<>();
    if (obj.tryGetValue("coordinates", coordRef)) {
      RavenJArray coordinates = (RavenJArray)coordRef.value;
      builder.value.append("MULTIPOINT (");
      if (coordinates != null && tryParseCoordinateArray(coordinates, builder))
      {
        builder.value.append(")");
        return true;
      }
    }
    return false;
  }

  private boolean tryParseMultiLineString(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<RavenJToken> coordRef = new Reference<>();
    if (obj.tryGetValue("coordinates", coordRef)) {
      RavenJArray coordinates = (RavenJArray) coordRef.value;
      builder.value.append("MULTILINESTRING (");
      if (coordinates != null && tryParseCoordinateArrayArray(coordinates, builder)) {
        builder.value.append(")");
        return true;
      }
    }
    return false;
  }

  private boolean tryParseMultiPolygon(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<RavenJToken> coordRef = new Reference<>();
    if (obj.tryGetValue("coordinates", coordRef)) {
      RavenJArray coordinates = (RavenJArray) coordRef.value;
      builder.value.append("MULTIPOLYGON (");
      if (coordinates != null && tryParseCoordinateArrayArrayArray(coordinates, builder))
      {
        builder.value.append(")");
        return true;
      }
    }
    return false;
  }

  private boolean tryParseGeometryCollection(RavenJObject obj, Reference<StringBuilder> builder) {
    Reference<RavenJToken> geomRef = new Reference<>();
    if (obj.tryGetValue("geometries", geomRef)) {
      RavenJArray geometries = (RavenJArray) geomRef.value;

      if (geometries != null) {
        builder.value.append("GEOMETRYCOLLECTION (");
        for (int index = 0; index < geometries.size(); index++) {
          if (index > 0) {
            builder.value.append(", ");
          }
          RavenJToken geometry = geometries.get(index);
          if (!tryParseGeometry((RavenJObject)geometry, builder)) {
            return false;
          }
        }
        builder.value.append(")");
        return true;
      }
    }
    return false;
  }

  private boolean tryParseCoordinate(RavenJArray coordinates, Reference<StringBuilder> result) {
    if (coordinates != null && coordinates.size() > 1) {
      for (RavenJToken token : coordinates) {
        if (!(token instanceof RavenJValue)) {
          return false;
        }
      }

      List<RavenJValue> vals = coordinates.values(RavenJValue.class);
      for (RavenJValue value: vals) {
        if (value.getType() != JTokenType.FLOAT && value.getType() != JTokenType.INTEGER) {
          return false;
        }
      }

      result.value.append(String.format(Constants.getDefaultLocale(), "%f %f", vals.get(0).value(Double.class), vals.get(1).value(Double.class)));
      return true;
    }
    return false;
  }

  private boolean tryParseCoordinateArray(RavenJArray coordinates, Reference<StringBuilder> result) {
    if (coordinates == null) {
      return false;
    }
    for (RavenJToken token: coordinates) {
      if (!(token instanceof RavenJArray)) {
        return false;
      }
    }

    for (int index = 0; index < coordinates.size(); index++) {
      if (index > 0) {
        result.value.append(", ");
      }
      if (!tryParseCoordinate((RavenJArray)coordinates.get(index), result)) {
        return false;
      }
    }
    return true;
  }

  private boolean tryParseCoordinateArrayArray(RavenJArray coordinates, Reference<StringBuilder> result) {
    if (coordinates == null) {
      return false;
    }

    for (RavenJToken token: coordinates) {
      if (!(token instanceof RavenJArray)) {
        return false;
      }
    }

    for (int index = 0; index < coordinates.size(); index++) {
      if (index > 0) {
        result.value.append(", ");
      }
      result.value.append("(");
      if (!tryParseCoordinateArray((RavenJArray)coordinates.get(index), result)) {
        return false;
      }
      result.value.append(")");
    }
    return true;
  }

  private boolean tryParseCoordinateArrayArrayArray(RavenJArray coordinates, Reference<StringBuilder> result) {
    if (coordinates == null) {
      return false;
    }


    for (RavenJToken token: coordinates) {
      if (!(token instanceof RavenJArray)) {
        return false;
      }
    }

    for (int index = 0; index < coordinates.size(); index++) {
      if (index > 0) {
        result.value.append(", ");
      }
      result.value.append("(");
      if (!tryParseCoordinateArrayArray((RavenJArray)coordinates.get(index), result)) {
        return false;
      }
      result.value.append(")");
    }
    return true;
  }

  public Object santizeRavenJObjects(Object obj) {
    if (obj instanceof RavenJArray) {
      RavenJArray ravenJArray = (RavenJArray) obj;
      List<Object> result = new ArrayList<>();
      for (RavenJToken token: ravenJArray) {
        result.add(santizeRavenJObjects(token));
      }
      return result.toArray();
    }
    if (obj instanceof RavenJObject) {
      RavenJObject ravenJObject = (RavenJObject) obj;
      Map<String, Object> map = new HashMap<>();
      for (Entry<String, RavenJToken> entry: ravenJObject) {
        map.put(entry.getKey(), santizeRavenJObjects(entry.getValue()));
      }
      return map;
    }

    return obj;
  }

}
