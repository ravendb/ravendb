package net.ravendb.client.converters;

public class Int64Converter implements ITypeConverter {

  @Override
  public boolean canConvertFrom(Class< ? > sourceType) {
    return Long.class.equals(sourceType) || Long.TYPE.equals(sourceType);
  }

  @Override
  public String convertFrom(String tag, Object value, boolean allowNull) {
    Long val = (Long) value;
    if (val == 0 && allowNull) {
      return null;
    }
    return tag + value;
  }

  @Override
  public Object convertTo(String value) {
    return Long.valueOf(value);
  }

}
