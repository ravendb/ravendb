package net.ravendb.abstractions.json.linq;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.print.Doc;

import net.ravendb.abstractions.basic.CollectionUtils;
import net.ravendb.abstractions.data.DocumentsChanges;
import net.ravendb.abstractions.data.DocumentsChanges.ChangeType;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.util.NetDateFormat;

import org.apache.commons.beanutils.ConvertUtils;

import com.google.common.base.Defaults;


public class Extensions {

  public static <U> U value(Class<U> clazz, Iterable<RavenJToken> value) {
    return value(RavenJToken.class, clazz, value);
  }

  public static <U> U value(Class<U> clazz, RavenJToken value) {
    return convert(clazz, value);
  }

  public static <U> Iterable<U> values(Class<U> clazz, Iterable<RavenJToken> source) {
    return values(clazz, source, null);
  }

  public static Iterable<RavenJToken> values(Iterable<RavenJToken> source, String key) {
    return values(RavenJToken.class, source, key);
  }

  public static Iterable<RavenJToken> values(Iterable<RavenJToken> source) {
    return values(source, null);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static <U> Iterable<U> values(Class<U> clazz, Iterable<RavenJToken> source, String key) {
    return new RavenJTokenIterable(clazz, source, key);
  }

  public static <T extends RavenJToken, U> U value(Class<T> clazzT, Class<U> clazzU, Iterable<T> value) //where T : RavenJToken
  {
    RavenJToken token = null;
    try {
      token = (RavenJToken) value;
    } catch (ClassCastException e) {
      //
    }
    if (token == null) throw new IllegalArgumentException("Source value must be a RavenJToken.");

    return convert(clazzU, token);
  }

  @SuppressWarnings("unchecked")
  public static <U> U convert(Class<U> clazz, RavenJToken token) {
    if (token.getType() == JTokenType.NULL) {
      return Defaults.defaultValue(clazz);
    }
    if (token instanceof RavenJArray && RavenJObject.class.equals(clazz)) {
      RavenJArray ar = (RavenJArray) token;
      RavenJObject o = new RavenJObject();
      for (RavenJToken item : ar) {
        RavenJToken key = ((RavenJObject) item).get("Key");
        RavenJToken value = ((RavenJObject) item).get("Value");
        o.set(value(String.class, key), value);

      }
      return (U) o;
    }
    boolean cast = RavenJToken.class.isAssignableFrom(clazz);
    return convert(clazz, token, cast);
  }

  public static <U> List<U> convert(Class<U> clazz, Iterable<RavenJToken> source) {
    boolean cast = RavenJToken.class.isAssignableFrom(clazz);

    List<U> result = new ArrayList<>();
    for (RavenJToken token : source) {
      result.add(convert(clazz, token, cast));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <U> U convert(Class<U> clazz, RavenJToken token, boolean cast) {
    if (cast || Object.class.equals(clazz)) {
      // HACK
      return (U) token;
    }
    if (token == null) {
      return null;
    }

    RavenJValue value;
    try {
      value = (RavenJValue) token;
    } catch (ClassCastException e) {
      throw new ClassCastException("Cannot cast " + token.getType() + " to " + clazz + ".");
    }

    if (value.getValue().getClass().equals(clazz)) {
      return (U) value.getValue();
    }

    if (!clazz.isPrimitive()) {
      if (value.getValue() == null) {
        return null;
      }
    }
    if (clazz.equals(UUID.class)) {
      //is it needed?
      if (value.getValue() == null) {
        return (U) UUID.randomUUID();
      }

      return (U) UUID.fromString(value.getValue().toString());
    }
    if (clazz.equals(Etag.class)) {
      if (value.getValue() == null) {
        return (U) new Etag();
      }
      return (U) Etag.parse(value.getValue().toString());
    }
    if (clazz.equals(String.class)) {
      if (value.getValue() == null) {
        return (U) new String();
      }
      return (U) value.getValue().toString();
    }
    if (clazz.equals(Date.class) && value.getValue().getClass().equals(String.class)) {
      NetDateFormat sdf = new NetDateFormat();
      try {
        return (U) sdf.parse((String) value.getValue());
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }

    }
    return (U) ConvertUtils.convert(value.getValue(), clazz);
  }

  private static class RavenJTokenIterable<U> implements Iterable<U> {

    private Class<U> clazz;
    private final Iterable<RavenJToken> source;
    private String key;

    public RavenJTokenIterable(Class<U> clazz, Iterable<RavenJToken> source, String key) {
      super();
      this.clazz = clazz;
      this.source = source;
      this.key = key;
    }

    @Override
    public Iterator<U> iterator() {
      return new RavenJTokenIterator<>();
    }

    private class RavenJTokenIterator<T> implements Iterator<T> {

      Iterator<RavenJToken> sourceIterator = source.iterator();

      @Override
      public boolean hasNext() {
        return sourceIterator.hasNext();
      }

      @SuppressWarnings("unchecked")
      @Override
      public T next() {
        RavenJToken token = sourceIterator.next();
        if (token instanceof RavenJValue) {
          return (T) convert(clazz, token);
        } else {
          for (U t : token.values(clazz)) {
            return (T) t;
          }
        }

        RavenJObject ravenJObject = (RavenJObject) token;

        RavenJToken value = ravenJObject.get(key);
        if (value != null) {
          return (T) convert(clazz, value);
        }
        return null;
      }

      @Override
      public void remove() {
        throw new IllegalStateException("Deleting elements in iterator is not implemneted!");
      }

    }

  }

  public static boolean compareRavenJArrayData(Collection<DocumentsChanges> docChanges, RavenJArray selfArray, RavenJArray otherArray, String fieldArrName) {

    List<RavenJToken> differences;
    if (selfArray.size() < otherArray.size()) {
      differences = CollectionUtils.except(otherArray, selfArray);
    } else {
      differences = CollectionUtils.except(selfArray, otherArray);
    }

    if (differences.isEmpty()) {
      return true;
    }

    for (RavenJToken dif : differences) {
      DocumentsChanges changes = new DocumentsChanges();
      changes.setFieldName(fieldArrName);

      if (selfArray.size() < otherArray.size()) {
        changes.setChange(ChangeType.ARRAY_VALUE_REMOVED);
        changes.setFieldOldValue(dif.toString());
        changes.setFieldOldType(dif.getType().toString());
      }

      if (selfArray.size() > otherArray.size()) {
        changes.setChange(ChangeType.ARRAY_VALUE_ADDED);
        changes.setFieldNewValue(dif.toString());
        changes.setFieldNewType(dif.getType().toString());
      }
      docChanges.add(changes);

    }

    return false;

  }

  public static boolean compareDifferentLengthRavenJObjectData(Collection<DocumentsChanges> docChanges, RavenJObject otherObj, RavenJObject selfObj, String fieldName) {
    Map<String, String> diffData = new HashMap<>();
    String descr = "";
    RavenJToken token;
    if (otherObj.getCount() == 0) {
      for (Map.Entry<String, RavenJToken> kvp: selfObj.getProperties()) {
        DocumentsChanges changes = new DocumentsChanges();
        if (selfObj.getProperties().containsKey(kvp.getKey())) {
          token = selfObj.getProperties().get(kvp.getKey());
          changes.setFieldNewValue(token.toString());
          changes.setFieldNewType(token.getType().toString());
          changes.setChange(ChangeType.NEW_FIELD);

          changes.setFieldName(kvp.getKey());
        }

        changes.setFieldOldValue("null");
        changes.setFieldOldType("null");

        docChanges.add(changes);
      }
      return false;
    }

    fillDifferentJsonData(selfObj.getProperties(), otherObj.getProperties(), diffData);

    for (String key: diffData.keySet()) {
      DocumentsChanges changes = new DocumentsChanges();
      changes.setFieldOldType(otherObj.getType().toString());
      changes.setFieldNewType(selfObj.getType().toString());
      changes.setFieldName(key);

      if (selfObj.getCount() < otherObj.getCount()) {
        changes.setChange(ChangeType.REMOVED_FIELD);
        changes.setFieldOldValue(diffData.get(key));
      } else {
        changes.setChange(ChangeType.NEW_FIELD);
        changes.setFieldNewValue(diffData.get(key));
      }
      docChanges.add(changes);
    }

    return false;
  }

  private static void fillDifferentJsonData(DictionaryWithParentSnapshot selfObj, DictionaryWithParentSnapshot otherObj, Map<String, String> diffData) {
    List<String> diffNames;
    DictionaryWithParentSnapshot bigObj;
    if (selfObj.size() < otherObj.size()) {
      diffNames = CollectionUtils.except(otherObj.keySet(), selfObj.keySet());
      bigObj = otherObj;
    } else {
      diffNames = CollectionUtils.except(selfObj.keySet(), otherObj.keySet());
      bigObj = selfObj;
    }
    for (String kvp : diffNames) {
      if (bigObj.containsKey(kvp)) {
        diffData.put(kvp, bigObj.get(kvp).toString());
      }
    }
  }

  public static void addChanges(List<DocumentsChanges> docChanges, DocumentsChanges.ChangeType change) {
    DocumentsChanges documentsChanges = new DocumentsChanges();
    documentsChanges.setChange(change);

    docChanges.add(documentsChanges);
  }

  public static void addChanges(List<DocumentsChanges> docChanges, String key, RavenJToken value, RavenJToken token) {
    DocumentsChanges docChange = new DocumentsChanges();
    docChange.setFieldNewType(value.getType().toString());
    docChange.setFieldOldType(token.getType().toString());
    docChange.setFieldNewValue(value.toString());
    docChange.setFieldOldValue(token.toString());
    docChange.setChange(ChangeType.FIELD_CHANGED);
    docChange.setFieldName(key);

    docChanges.add(docChange);
  }

  public static void addChanges(List<DocumentsChanges> docChanges, String key, RavenJToken value, RavenJToken token, String fieldName) {
    DocumentsChanges docChange = new DocumentsChanges();
    docChange.setFieldNewType(value.getType().toString());
    docChange.setFieldOldType(token.getType().toString());
    docChange.setFieldNewValue(value.toString());
    docChange.setFieldOldValue(token.toString());
    docChange.setChange(ChangeType.FIELD_CHANGED);
    docChange.setFieldName(fieldName);

    docChanges.add(docChange);
  }

  public static void addChanges(List<DocumentsChanges> docChanges, RavenJToken curThisReader, RavenJToken curOtherReader, String fieldName) {
    DocumentsChanges changes = new DocumentsChanges();
    changes.setFieldNewType(curThisReader.getType().toString());
    changes.setFieldOldType(curOtherReader.getType().toString());
    changes.setFieldNewValue(curThisReader.toString());
    changes.setFieldOldValue(curOtherReader.toString());
    changes.setChange(ChangeType.FIELD_CHANGED);
    changes.setFieldName(fieldName);

    docChanges.add(changes);

  }
}
