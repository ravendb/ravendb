package raven.abstractions.json.linq;


public class Extensions {

  public static <U> Iterable<U> convert(Class<U> clazz, Iterable<RavenJToken> source)
  {
      boolean cast = RavenJToken.class.isAssignableFrom(clazz);

      //TODO: implement me
      return null;
  }

  public static <U> U convert(Class<U> clazz, RavenJToken token, boolean cast)
  {
      if (cast)
      {
          // HACK
          return (U)(Object)token;
      }
      if (token == null)
          return null;

      RavenJValue value;
      try {
        value = (RavenJValue) token;
      } catch (ClassCastException e) {
        throw new ClassCastException("Cannot cast " + token.getType() + " to " + clazz + ".");
      }

      if (value.getValue().getClass().isAssignableFrom(clazz)){
          return (U)value.getValue();
      }

    //TODO: implement me
      return null;
  }


}
