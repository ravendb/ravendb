package raven.abstractions.json.linq;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.beanutils.ConvertUtils;

import raven.abstractions.data.Constants;

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

  //TODO: fixme
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

  private static <U> U convert(Class<U> clazz, RavenJToken token) {
    if (token instanceof RavenJArray && RavenJObject.class.equals(clazz)) {
      RavenJArray ar = (RavenJArray) token;
      RavenJObject o = new RavenJObject();
      for (RavenJToken item : ar) {
        RavenJToken key = ((RavenJObject) item).get("Key");
        RavenJToken value = ((RavenJObject) item).get("Value");
        o.set(value(String.class, key), value);

      }
      return (U) (Object) o;
    }
    boolean cast = RavenJToken.class.isAssignableFrom(clazz);
    return convert(clazz, token, cast);
  }

  public static <U> Iterable<U> convert(Class<U> clazz, Iterable<RavenJToken> source) {
    boolean cast = RavenJToken.class.isAssignableFrom(clazz);

    List<U> result = new ArrayList<>();
    for (RavenJToken token : source) {
      result.add(convert(clazz, token, cast));
    }
    return result;
  }

  public static <U> U convert(Class<U> clazz, RavenJToken token, boolean cast) {
    if (cast || Object.class.equals(clazz)) {
      // HACK
      return (U) (Object) token;
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

    //Type targetType = typeof(U);
    //TODO: fixme
    if (!clazz.isPrimitive()) {
      if (value.getValue() == null) {
        try {
          return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          //TODO: implement
        }
      }

      //targetType = Nullable.GetUnderlyingType(targetType);
    }
    if (clazz.equals(UUID.class)) {
      //is it needed?
      if (value.getValue() == null) {
        return (U) UUID.randomUUID();
      }

      return (U) UUID.fromString(value.getValue().toString());
    }
    if (clazz.equals(String.class)) {
      if (value.getValue() == null) {
        try {
          return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          //TODO: implement
        }
      }
      return (U) (Object) value.getValue().toString();
    }
    if (clazz.equals(Date.class) && value.getValue().getClass().equals(String.class)) {
      SimpleDateFormat sdf = new SimpleDateFormat(Constants.RAVEN_S_DATE_FORMAT);
      try {
        return (U) (Object) sdf.parse((String) value.getValue());
      } catch (ParseException e) {
        // TODO implement
      }

    }
    //TODO:
    /*
    if (targetType == typeof(DateTimeOffset) && value.Value is string)
    {
        DateTimeOffset dateTimeOffset;
        if (DateTimeOffset.TryParseExact((string)value.Value, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeOffset))
            return (U)(object)dateTimeOffset;

        return default(U);
    }*/
    //return (U)System.Convert.ChangeType(value.Value, targetType, CultureInfo.InvariantCulture);
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
      return new RavenJTokenIterator<U>();
    }

    private class RavenJTokenIterator<T> implements Iterator<T> {

      Iterator<RavenJToken> sourceIterator = source.iterator();

      @Override
      public boolean hasNext() {
        // TODO Auto-generated method stub
        return sourceIterator.hasNext();
      }

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



}
