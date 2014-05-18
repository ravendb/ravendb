package net.ravendb.abstractions.json.linq;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;

import net.ravendb.abstractions.exceptions.JsonWriterException;
import net.ravendb.abstractions.util.NetDateFormat;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.JsonGenerator;


public class RavenJValue extends RavenJToken {

  private JTokenType valueType;
  private Object value;
  private boolean snapshot;

  /**
   * Gets the node type for this {@link RavenJToken}
   */
  @Override
  public JTokenType getType() {
    return valueType;
  }

  /**
   * Gets the underlying token value.
   * @return
   */
  public Object getValue() {
    return value;
  }

  /**
   * Sets new value and type of {@link RavenJValue}.
   * @param value
   * @throws IllegalStateException if instance is snapshot
   * @throws IllegalArgumentException if <code>value</code> class is not supported.
   */
  public void setValue(Object value) {
    if (snapshot) {
      throw new IllegalStateException("Cannot modify a snapshot, this is probably a bug");
    }

    Class< ? > currentClass = (this.value != null) ? this.value.getClass() : null;
    Class< ? > newClass = (value != null) ? value.getClass() : null;

    if (currentClass == null || !currentClass.equals(newClass)) {
      valueType = getValueType(valueType, value);
    }

    this.value = value;

  }

  protected RavenJValue(Object value, JTokenType type) {
    this.value = value;
    this.valueType = type;
  }

  @Override
  public RavenJValue cloneToken() {
    return new RavenJValue(value, valueType);
  }

  @Override
  public boolean isSnapshot() {
    return snapshot;
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(int value) {
    this(value, JTokenType.INTEGER);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(long value) {
    this(value, JTokenType.INTEGER);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(double value) {
    this(value, JTokenType.FLOAT);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(float value) {
    this(value, JTokenType.FLOAT);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(Date value) {
    this(value, JTokenType.DATE);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(boolean value) {
    this(value, JTokenType.BOOLEAN);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(String value) {
    this(value, JTokenType.STRING);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(URI value) {
    this(value, JTokenType.STRING);
  }

  /**
   * Initializes a new instance of the {@link RavenJValue} class with the given value.
   * @param value
   */
  public RavenJValue(Object value) {
    this(value, getValueType(null, value));
  }

  private static JTokenType getValueType(JTokenType current, Object value) {
    if (value == null) {
      return JTokenType.NULL;
    } else if (value instanceof String) {
      return JTokenType.STRING;
    } else if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
      return JTokenType.INTEGER;
    } else if (value instanceof Enum) {
      return JTokenType.INTEGER;
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      return JTokenType.FLOAT;
    } else if (value instanceof Date) {
      return JTokenType.INTEGER;
    } else if (value instanceof byte[]) {
      return JTokenType.BYTES;
    } else if (value instanceof Boolean) {
      return JTokenType.BOOLEAN;
    } else if (value instanceof URI) {
      return JTokenType.STRING;
    }

    throw new IllegalArgumentException("Could not determine JSON object type for class " + value.getClass().getCanonicalName());

  }

  @Override
  public int hashCode() {
    return deepHashCode();
  }

  @Override
  public int deepHashCode() {
    final int prime = 31;
    int result = 1;

    if (valueType != null && valueType == JTokenType.BYTES) {
      result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
      if (value != null) {
        byte[] bytes = (byte[]) value;
        for (int i = 0; i < bytes.length; i++) {
          result = prime * result + bytes[i];
        }
      }
    } else {
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RavenJValue other = (RavenJValue) obj;
    return deepEquals(other);
  }

  @Override
  public void ensureCannotBeChangeAndEnableShapshotting() {
    snapshot = true;
  }

  @Override
  public RavenJValue createSnapshot() {
    if (!snapshot) {
      throw new IllegalStateException("Cannot create snapshot without previously calling EnsureSnapShot");
    }
    return new RavenJValue(value, valueType);
  }

  public static RavenJValue getNull() {
    return new RavenJValue(null, JTokenType.NULL);
  }

  @Override
  public void writeTo(JsonGenerator writer) {
    try {
      if (value == null) {
        writer.writeNull();
        return;
      }
      switch (valueType) {
        case NULL:
          writer.writeNull();
          return;
        case BOOLEAN:
          writer.writeBoolean((Boolean) value);
          return;
        case BYTES:
          writer.writeBinary((byte[]) value);
          return;
        case FLOAT:
          if (value instanceof Double) {
            writer.writeNumber((Double) value);
          } else if (value instanceof BigDecimal) {
            writer.writeNumber((BigDecimal) value);
          } else {
            throw new JsonWriterException("Unexpected numeric class: " + value.getClass());
          }
          return;
        case INTEGER:
          if (value instanceof Long) {
            writer.writeNumber((Long) value);
          } else if (value instanceof Integer) {
            writer.writeNumber((Integer) value);
          } else if (value instanceof BigInteger) {
            writer.writeNumber((BigInteger) value);
          } else {
            throw new JsonWriterException("Unexpected numeric class: " + value.getClass());
          }

          return;
        case STRING:
          writer.writeString((String) value);
          return;
        case DATE:
          writer.writeString(new NetDateFormat().format(value));
          break;
        default:
          throw new JsonWriterException("Unexpected token:" + valueType);
      }
    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(), e);
    }

  }

  @Override
  public String toString() {
    if (value == null) {
      return "";
    }
    return value.toString();
  }

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#deepEquals(raven.client.json.RavenJToken)
   */
  @Override
  public boolean deepEquals(RavenJToken node) {
    if (getType() == JTokenType.NULL && (node == null || node.getType() == JTokenType.NULL)) {
      return true;
    }
    if (!(node instanceof RavenJValue)) {
      return false;
    }
    RavenJValue other = (RavenJValue) node;
    if (this == other) {
      return true;
    }
    return valuesEquals(this, other);
  }

  private static boolean valuesEquals(RavenJValue v1, RavenJValue v2) {
    if (v1.getType() == v2.getType() && v1.getType() != JTokenType.BYTES && v1.getValue() != null && v1.getValue().equals(v2.getValue())) {
      return true;
    }
    if (v1.getType() == JTokenType.NULL || v2.getType() == JTokenType.NULL) {
      // if both are null we return true
      return false;
    }

    // please note that already check for equality when items has the same type
    // the only change to return true left in different types
    switch (v1.getType()) {
      case INTEGER:
      case FLOAT:
        if (v2.getType() != JTokenType.INTEGER && v2.getType() != JTokenType.FLOAT) {
          return false;
        }
        return compareNumbers(v1, v2);

      case STRING:
        if (v2.getType() == JTokenType.BYTES) {
          return compareBytes(v1, v2);
        }
        return false;
      case BYTES:
        if (v2.getType() != JTokenType.STRING && v2.getType() != JTokenType.BYTES) {
          return false;
        }
        return compareBytes(v1, v2);
    }

    return false;
  }

  private static boolean compareBytes(RavenJValue v1, RavenJValue v2) {
    byte[] arr1 = v1.asBytesArray();
    byte[] arr2 = v2.asBytesArray();

    return arr1 != null && arr2 != null && Arrays.equals(arr1, arr2);
  }

  private byte[] asBytesArray() {
    if (getType() == JTokenType.BYTES) {
      if (getValue() != null && getValue() instanceof byte[]) {
        return (byte[]) getValue();
      }
    }
    if (getType() == JTokenType.STRING) {
      if (getValue() != null && getValue() instanceof String) {
        String stringValue = (String) getValue();
        if (Base64.isBase64(stringValue)) {
          return Base64.decodeBase64(stringValue);
        }
      }
    }
    return null;
  }

  private static boolean compareNumbers(RavenJValue v1, RavenJValue v2) {
    if (v1.getType() == JTokenType.FLOAT || v2.getType() == JTokenType.FLOAT) {
      return compareFloats(v1, v2);
    }
    // compare as integers
    Number ov1 = (Number) v1.getValue();
    Number ov2 = (Number) v2.getValue();
    if (ov1 == null || ov2 == null) {
      return false;
    }

    if (ov1 instanceof BigInteger || ov2 instanceof BigInteger) {
      return v1.asBigInteger().compareTo(v2.asBigInteger()) == 0;
    } else if (ov1 instanceof Long || ov2 instanceof Long) {
      return ov1.longValue() == ov2.longValue();
    } else if (ov1 instanceof Integer || ov2 instanceof Integer) {
      return ov1.intValue() == ov2.intValue();
    } else if (ov1 instanceof Short || ov2 instanceof Short) {
      return ov1.shortValue() == ov2.shortValue();
    } else {
      throw new IllegalStateException("Cannot find common numeric class for " + ov1.getClass() + " and" + ov2.getClass());
    }
  }

  private BigInteger asBigInteger() {
    if (getType() != JTokenType.INTEGER) {
      return null;
    }
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    } else if (value instanceof Long || value instanceof Integer || value instanceof Short) {
      Number number = (Number) value;
      return BigInteger.valueOf(number.longValue());
    }
    return null;
  }

  private static boolean compareFloats(RavenJValue v1, RavenJValue v2) {
    Number ov1 = (Number) v1.getValue();
    Number ov2 = (Number) v2.getValue();
    if (ov1 == null || ov2 == null) {
      return false;
    }

    // make sure both numbers are float
    if (ov1 instanceof BigDecimal) {
      // do nothing
    } else if (ov1 instanceof BigInteger) {
      ov1 = new BigDecimal((BigInteger) ov1);
    } else {
      ov1 = ov1.doubleValue();
    }

    if (ov2 instanceof BigDecimal) {
      // do nothing
    } else if (ov2 instanceof BigInteger) {
      ov2 = new BigDecimal((BigInteger) ov2);
    } else {
      ov2 = ov2.doubleValue();
    }

    if (ov1 instanceof BigDecimal || ov2 instanceof BigDecimal) {
      return v1.asBigDecimal().subtract(v2.asBigDecimal()).abs().compareTo(new BigDecimal("0.000001")) < 0;
    } else if (ov1 instanceof Double || ov2 instanceof Double) {
      return Math.abs(ov1.doubleValue() - ov2.doubleValue()) < 0.000001;
    } else if (ov1 instanceof Float || ov2 instanceof Float) {
      return ov1.floatValue() == ov2.floatValue();
    } else {
      throw new IllegalStateException("Cannot find common numeric class for " + ov1.getClass() + " and" + ov2.getClass());
    }
  }

  private BigDecimal asBigDecimal() {
    Number number = (Number) value;
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof Double) {
      return new BigDecimal((double) value);
    } else if (value instanceof Float) {
      return new BigDecimal(number.doubleValue());
    } else {
      return new BigDecimal(asBigInteger());
    }
  }

}
