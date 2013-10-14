package net.ravendb.client.converters;

/**
 * Interface for performing type conversions.
 * We couldn't use the built-in TypeConverter because it is too big an interface for people to build on.
 */
public interface ITypeConverter {

  /**
   * Returns whether this converter can convert an object of the given type to the type of this converter.
   * @param sourceType
   * @return true if this converter can perform the conversion; otherwise, false.
   */
  public boolean canConvertFrom(Class<?> sourceType);

  /**
   * Converts the given object to the type of this converter.
   * @param tag
   * @param value
   * @param allowNull
   * @return
   */
  public String convertFrom(String tag, Object value, boolean allowNull);

  /**
   * Converts the given value object to the specified type, using the specified context and culture information.
   * @param value
   * @return
   */
  public Object convertTo(String value);
}
