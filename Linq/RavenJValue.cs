using System;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
    public class RavenJValue : RavenJToken
    {
        private JTokenType _valueType;
        private object _value;

        /// <summary>
        /// Gets the node type for this <see cref="RavenJToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public override JTokenType Type
        {
            get { return _valueType; }
        }

        /// <summary>
        /// Gets or sets the underlying token value.
        /// </summary>
        /// <value>The underlying token value.</value>
        public object Value
        {
            get { return _value; }
            set
            {
                Type currentType = (_value != null) ? _value.GetType() : null;
                Type newType = (value != null) ? value.GetType() : null;

                if (currentType != newType)
                    _valueType = GetValueType(_valueType, value);

                _value = value;
            }
        }

        internal RavenJValue(object value, JTokenType type)
        {
            _value = value;
            _valueType = type;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class from another <see cref="RavenJValue"/> object.
        /// </summary>
        /// <param name="other">A <see cref="JValue"/> object to copy from.</param>
        public RavenJValue(RavenJValue other)
            : this(other.Value, other.Type)
        {
        }

        public override RavenJToken CloneToken()
        {
            return new RavenJValue(Value, Type);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        public RavenJValue(int value)
            : this(value, JTokenType.Integer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        public RavenJValue(long value)
            : this(value, JTokenType.Integer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        [CLSCompliant(false)]
        public RavenJValue(ulong value)
            : this(value, JTokenType.Integer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        public RavenJValue(double value)
            : this(value, JTokenType.Float)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        public RavenJValue(DateTime value)
            : this(value, JTokenType.Date)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        public RavenJValue(bool value)
            : this(value, JTokenType.Boolean)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        public RavenJValue(string value)
            : this(value, JTokenType.String)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJValue"/> class with the given value.
        /// </summary>
        /// <param name="value">The value.</param>
        public RavenJValue(object value)
            : this(value, GetValueType(null, value))
        {
        }

        private static JTokenType GetValueType(JTokenType? current, object value)
        {
            if (value == null)
                return JTokenType.Null;
            else if (value == DBNull.Value)
                return JTokenType.Null;
            else if (value is string)
                return GetStringValueType(current);
            else if (value is long || value is int || value is short || value is sbyte
              || value is ulong || value is uint || value is ushort || value is byte)
                return JTokenType.Integer;
            else if (value is Enum)
                return JTokenType.Integer;
            else if (value is double || value is float || value is decimal)
                return JTokenType.Float;
            else if (value is DateTime)
                return JTokenType.Date;
#if !PocketPC && !NET20
            else if (value is DateTimeOffset)
                return JTokenType.Date;
#endif
            else if (value is byte[])
                return JTokenType.Bytes;
            else if (value is bool)
                return JTokenType.Boolean;

            throw new ArgumentException("Could not determine JSON object type for type {0}.".FormatWith(CultureInfo.InvariantCulture, value.GetType()));
        }

        private static JTokenType GetStringValueType(JTokenType? current)
        {
            if (current == null)
                return JTokenType.String;

            switch (current.Value)
            {
                case JTokenType.Comment:
                case JTokenType.String:
                case JTokenType.Raw:
                    return current.Value;
                default:
                    return JTokenType.String;
            }
        }

        internal static RavenJValue Load(JsonReader reader)
        {
            RavenJValue v;
            switch (reader.TokenType)
            {
                case JsonToken.String:
                case JsonToken.Integer:
                case JsonToken.Float:
                case JsonToken.Date:
                case JsonToken.Boolean:
                case JsonToken.Bytes:
                    v = new RavenJValue(reader.Value);
                    break;
                case JsonToken.Null:
                    v = new RavenJValue(null, JTokenType.Null);
                    break;
                case JsonToken.Undefined:
                    v = new RavenJValue(null, JTokenType.Undefined);
                    break;
                default:
                    throw new InvalidOperationException("The JsonReader should not be on a token of type {0}."
                                                            .FormatWith(CultureInfo.InvariantCulture,
                                                                        reader.TokenType));
            }
            return v;
        }

        /// <summary>
        /// Writes this token to a <see cref="JsonWriter"/>.
        /// </summary>
        /// <param name="writer">A <see cref="JsonWriter"/> into which this method will write.</param>
        /// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
        /*public override void WriteTo(JsonWriter writer, params JsonConverter[] converters)
        {
            switch (_valueType)
            {
                case JTokenType.Comment:
                    writer.WriteComment(_value.ToString());
                    return;
                case JTokenType.Raw:
                    writer.WriteRawValue((_value != null) ? _value.ToString() : null);
                    return;
                case JTokenType.Null:
                    writer.WriteNull();
                    return;
                case JTokenType.Undefined:
                    writer.WriteUndefined();
                    return;
            }

            JsonConverter matchingConverter;
            if (_value != null && ((matchingConverter = JsonSerializer.GetMatchingConverter(converters, _value.GetType())) != null))
            {
                matchingConverter.WriteJson(writer, _value, new JsonSerializer());
                return;
            }

            switch (_valueType)
            {
                case JTokenType.Integer:
                    writer.WriteValue(Convert.ToInt64(_value, CultureInfo.InvariantCulture));
                    return;
                case JTokenType.Float:
                    writer.WriteValue(Convert.ToDouble(_value, CultureInfo.InvariantCulture));
                    return;
                case JTokenType.String:
                    writer.WriteValue((_value != null) ? _value.ToString() : null);
                    return;
                case JTokenType.Boolean:
                    writer.WriteValue(Convert.ToBoolean(_value, CultureInfo.InvariantCulture));
                    return;
                case JTokenType.Date:
#if !PocketPC && !NET20
                    if (_value is DateTimeOffset)
                        writer.WriteValue((DateTimeOffset)_value);
                    else
#endif
                        writer.WriteValue(Convert.ToDateTime(_value, CultureInfo.InvariantCulture)); ;
                    return;
                case JTokenType.Bytes:
                    writer.WriteValue((byte[])_value);
                    return;
            }

            throw MiscellaneousUtils.CreateArgumentOutOfRangeException("TokenType", _valueType, "Unexpected token type.");
        }*/
    }
}