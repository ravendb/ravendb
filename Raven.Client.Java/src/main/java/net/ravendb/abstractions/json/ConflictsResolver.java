package net.ravendb.abstractions.json;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJTokenComparator;


public class ConflictsResolver {

  private RavenJObject[] docs;
  private RavenJTokenComparator ravenJTokenComparator = new RavenJTokenComparator();

  public ConflictsResolver(RavenJObject... docs) {
    this.docs = docs;
  }

  public String resolve() {
    return resolve(1);
  }

  public String resolve(int indent) {
    Map<String, Object> result = new HashMap<>();
    for (int index = 0; index < docs.length; index++) {
      RavenJObject doc = docs[index];
      for (Entry<String, RavenJToken> prop : doc) {
        if (result.containsKey(prop.getKey())) { // already dealt with
          continue;
        }

        JTokenType type = prop.getValue().getType();
        boolean executeDefault = true;
        if (type == JTokenType.OBJECT && tryHandleObjectValue(index, result, prop)) {
          executeDefault = false;
        }
        if (type == JTokenType.ARRAY && tryHandleArrayValue(index, result, prop)) {
          executeDefault = false;
        }
        if (executeDefault) {
          handleSimpleValues(result, prop, index);
        }
      }
    }
    return generateOutput(result, indent);
  }

  private boolean tryHandleArrayValue(int index, Map<String, Object> result, Map.Entry<String, RavenJToken> prop) {
    List<RavenJArray> arrays = new ArrayList<>();
    arrays.add((RavenJArray) prop.getValue());

    for (int i = 0; i < docs.length; i++) {
      if (i == index) {
        continue;
      }

      RavenJToken token = null;
      if (docs[i].containsKey(prop.getKey())) {
        token = docs[i].get(prop.getKey());
        if (token.getType() != JTokenType.ARRAY) {
          return false;
        }
        arrays.add((RavenJArray) token);
      }
    }

    RavenJArray mergedArray = new RavenJArray();
    while (arrays.size() > 0) {
      Set<RavenJToken> set = new TreeSet<>(ravenJTokenComparator);
      for (int i = 0; i < arrays.size(); i++) {
        if (arrays.get(i).size() == 0) {
          arrays.remove(i);
          i--;
          continue;
        }
        set.add(arrays.get(i).get(0));
        arrays.get(i).removeAt(0);
      }

      for (RavenJToken ravenJToken : set) {
        mergedArray.add(ravenJToken);
      }
    }

    if (ravenJTokenComparator.compare(mergedArray, prop.getValue()) == 0) {
      result.put(prop.getKey(), mergedArray);
      return true;
    }

    result.put(prop.getKey(), new ArrayWithWarning(mergedArray));
    return true;

  }

  private boolean tryHandleObjectValue(int index, Map<String, Object> result, Map.Entry<String, RavenJToken> prop) {
    List<RavenJObject> others = new ArrayList<>();
    others.add((RavenJObject) prop.getValue());

    for (int i = 0; i < docs.length; i++) {
      if (i == index) {
        continue;
      }

      RavenJToken token = null;
      if (docs[i].containsKey(prop.getKey())) {
        token = docs[i].get(prop.getKey());
        if (token.getType() != JTokenType.OBJECT) {
          return false;
        }
        others.add((RavenJObject)token);
      }
    }
    result.put(prop.getKey(), new ConflictsResolver(others.toArray(new RavenJObject[0])));
    return true;
  }


  private void handleSimpleValues(Map<String, Object> result, Map.Entry<String, RavenJToken> prop, int index) {
    Conflicted conflicted = new Conflicted();
    conflicted.getValues().add(prop.getValue());

    for (int i = 0; i < docs.length; i++) {
      if (i == index) {
        continue;
      }
      RavenJObject other = docs[i];
      RavenJToken otherVal = null;
      if (!other.containsKey(prop.getKey())) {
        continue;
      }
      otherVal = other.get(prop.getKey());

      if (ravenJTokenComparator.compare(prop.getValue(), otherVal) != 0) {
        conflicted.getValues().add(otherVal);
      }
    }
    if (conflicted.getValues().size() == 1) {
      result.put(prop.getKey(), prop.getValue());
    } else {
      result.put(prop.getKey(), conflicted);
    }
  }

  private static String generateOutput(Map<String, Object> result, int indent) {
    try {
      JsonFactory factory = new JsonFactory();
      StringWriter stringWriter = new StringWriter();
      JsonGenerator writer = factory.createJsonGenerator(stringWriter);

      writer.writeStartObject();
      for (Map.Entry<String, Object> o : result.entrySet()) {
        writer.writeFieldName(o.getKey());
        if (o.getValue() instanceof RavenJToken) {
          RavenJToken ravenJToken = (RavenJToken) o.getValue();
          ravenJToken.writeTo(writer);
          continue;
        }
        if (o.getValue() instanceof Conflicted) {
          Conflicted conflicted = (Conflicted) o.getValue();
          writer.writeRaw("/* >>>> conflict start */");
          writer.writeStartArray();
          for (RavenJToken token : conflicted.getValues()) {
            token.writeTo(writer);
          }
          writer.writeEndArray();
          writer.writeRaw("/* <<<< conflict end */");
          continue;
        }

        if (o.getValue() instanceof ArrayWithWarning) {
          ArrayWithWarning arrayWithWarning = (ArrayWithWarning) o.getValue();
          writer.writeRaw("/* >>>> auto merged array start */");
          arrayWithWarning.getMergedArray().writeTo(writer);
          writer.writeRaw("/* <<<< auto merged array end */");
          continue;
        }

        if (o.getValue() instanceof ConflictsResolver) {
          ConflictsResolver resolver = (ConflictsResolver) o.getValue();

          BufferedReader stringReader = new BufferedReader(new StringReader(resolver.resolve(indent + 1)));
          boolean first = true;
          String line = null;
          while ((line = stringReader.readLine()) != null) {
            if (!first) {
              writer.writeRaw(System.lineSeparator());
              for (int i = 0; i < indent; i++) {
                writer.writeRaw(" ");
              }
            }
            if (first) {
              writer.writeRawValue(line);
            } else {
              writer.writeRaw(line);
            }

            first = false;
          }

          continue;
        }

        throw new IllegalStateException("Could not understand how to deal with: " + o.getValue());

      }
      writer.writeEndObject();
      writer.close();
      return stringWriter.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public static class Conflicted {
    private final Set<RavenJToken> values = new TreeSet<>(new RavenJTokenComparator());

    public Set<RavenJToken> getValues() {
      return values;
    }


  }

  public static class ArrayWithWarning {
    private final RavenJArray mergedArray;

    public RavenJArray getMergedArray() {
      return mergedArray;
    }

    public ArrayWithWarning(RavenJArray mergedArray) {
      super();
      this.mergedArray = mergedArray;
    }
  }
}
