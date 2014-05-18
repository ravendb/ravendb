package net.ravendb.client.converters;

public class Int32Converter implements ITypeConverter {

  @Override
  public boolean canConvertFrom(Class< ? > sourceType) {
    return Integer.class.equals(sourceType) || Integer.TYPE.equals(sourceType);
  }

  @Override
  public String convertFrom(String tag, Object value, boolean allowNull) {
    Integer val = (Integer) value;
    if (val == 0 && allowNull) {
      return null;
    }
    return tag + value;
  }

  @Override
  public Object convertTo(String value) {
    return Integer.valueOf(value);
  }

}
