package raven.client.json;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.Date;

import org.codehaus.jackson.JsonGenerator;

import raven.client.json.lang.JsonWriterException;

public class RavenJValue extends RavenJToken {


  private JTokenType valueType;
  private Object value;
  private boolean snapshot;

  /**
   * Gets the node type for this {@link RavenJToken}
   */
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

    Class<?> currentClass = (this.value != null) ? this.value.getClass() : null;
    Class<?> newClass = (value != null) ? value.getClass() : null;

    if (currentClass == null || !currentClass.equals(newClass)) {
      valueType =  getValueType(valueType, value);
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
  public RavenJValue(Guid value) {
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
      return getStringValueType(current);
    } else if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
      return JTokenType.INTEGER;
    } else if (value instanceof Enum) {
      return JTokenType.INTEGER;
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      return JTokenType.FLOAT;
    } else if (value instanceof Date) {
      return JTokenType.DATE;
    } else if (value instanceof byte[]) {
      return JTokenType.BYTES;
    } else if (value instanceof Boolean) {
      return JTokenType.BOOLEAN;
    } else if (value instanceof URI) {
      return JTokenType.URI;
    } else if (value instanceof Guid) {
      return JTokenType.GUID;
    }

    throw new IllegalArgumentException("Could not determine JSON object type for class " + value.getClass().getCanonicalName());


  }

  private static JTokenType getStringValueType(JTokenType current) {
    if (current == null) {
      return JTokenType.STRING;
    }
    switch (current) {
    case COMMENT:
    case STRING:
    case RAW:
      return current;
    default:
      return JTokenType.STRING;
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    //TODO: correct impl!
    final int prime = 31;
    int result = 1;
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {

    //TODO: correct impl!
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RavenJValue other = (RavenJValue) obj;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    if (valueType != other.valueType)
      return false;
    return true;
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
      case RAW:
        writer.writeRaw(value.toString());
        return;
      case NULL:
        writer.writeNull();
        return;
      case BOOLEAN:
        writer.writeBoolean((Boolean)value);
        return;
      case BYTES:
        writer.writeBinary((byte[]) value);
        return;
      case DATE:
        //TODO:
        return;
      case FLOAT:
        writer.writeNumber((Double)value);
        return ;
      case INTEGER:
        if (value instanceof Long) {
        writer.writeNumber((Long)value);
        } else if (value instanceof Integer) {
          writer.writeNumber((Integer)value);
        } else  {
          throw new JsonWriterException("Unexpected numeric class: " + value.getClass());
        }

        return;
      case STRING:
        writer.writeString((String) value);
        return;
        //TODO finish me!
      default:
        throw new JsonWriterException("Unexpected token:" + valueType);
      }
    } catch (IOException e) {
      throw new JsonWriterException(e.getMessage(), e);
    }

  }

  @Override
  public String toString() {
    if (value == null){
      return "";
    }
    return value.toString();
  }


  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#deepEquals(raven.client.json.RavenJToken)
   */
  @Override
  public boolean deepEquals(RavenJToken other) {
    // TODO Auto-generated method stub
    return super.deepEquals(other);
  }

  /* (non-Javadoc)
   * @see raven.client.json.RavenJToken#deepHashCode()
   */
  @Override
  public int deepHashCode() {
    // TODO Auto-generated method stub
    return super.deepHashCode();
  }





  //TODO: private static bool ValuesEquals(RavenJValue v1, RavenJValue v2)
  //TODO: comparator

}
