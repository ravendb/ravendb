using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
	/// <summary>
	/// Represents a value in JSON (string, integer, date, etc).
	/// </summary>
	public class RavenJValue : RavenJToken, IEquatable<RavenJValue>, IFormattable, IComparable, IComparable<RavenJValue>
	{
		private JTokenType _valueType;
		private object _value;
		private bool isSnapshot;

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
				if (isSnapshot)
					throw new InvalidOperationException("Cannot modify a snapshot, this is probably a bug");

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
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
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
		/// Initializes a new instance of the <see cref="JValue"/> class with the given value.
		/// </summary>
		/// <param name="value">The value.</param>
		public RavenJValue(Guid value)
			: this(value, JTokenType.String)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="JValue"/> class with the given value.
		/// </summary>
		/// <param name="value">The value.</param>
		public RavenJValue(Uri value)
			: this(value, JTokenType.String)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="JValue"/> class with the given value.
		/// </summary>
		/// <param name="value">The value.</param>
		public RavenJValue(TimeSpan value)
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
			else if (value is Guid)
				return JTokenType.Guid;
			else if (value is Uri)
				return JTokenType.Uri;
			else if (value is TimeSpan)
				return JTokenType.TimeSpan;

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

		public new static RavenJValue Load(JsonReader reader)
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

		// Taken from Newtonsoft's Json.NET JsonSerializer
		internal static JsonConverter GetMatchingConverter(IList<JsonConverter> converters, Type objectType)
		{
			if (objectType == null)
				throw new ArgumentNullException("objectType");

			if (converters != null)
			{
				for (int i = 0; i < converters.Count; i++)
				{
					JsonConverter converter = converters[i];

					if (converter.CanConvert(objectType))
						return converter;
				}
			}

			return null;
		}

		/// <summary>
		/// Writes this token to a <see cref="JsonWriter"/>.
		/// </summary>
		/// <param name="writer">A <see cref="JsonWriter"/> into which this method will write.</param>
		/// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
		public override void WriteTo(JsonWriter writer, params JsonConverter[] converters)
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
			if (_value != null && ((matchingConverter = GetMatchingConverter(converters, _value.GetType())) != null))
			{
				matchingConverter.WriteJson(writer, _value, JsonExtensions.CreateDefaultJsonSerializer());
				return;
			}

			switch (_valueType)
			{
				case JTokenType.Integer:
					writer.WriteValue(Convert.ToInt64(_value, CultureInfo.InvariantCulture));
					return;
				case JTokenType.Float:
					if (_value is decimal)
					{
						writer.WriteValue((decimal)_value);
						return;
					}
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
						writer.WriteValue(Convert.ToDateTime(_value, CultureInfo.InvariantCulture));
					return;
				case JTokenType.Bytes:
					writer.WriteValue((byte[])_value);
					return;
				case JTokenType.Guid:
				case JTokenType.Uri:
				case JTokenType.TimeSpan:
					writer.WriteValue((_value != null) ? _value.ToString() : null);
					return;
			}

			throw MiscellaneousUtils.CreateArgumentOutOfRangeException("TokenType", _valueType, "Unexpected token type.");
		}

		/// <summary>
		/// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
		/// </summary>
		/// <param name="obj">The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/>.</param>
		/// <returns>
		/// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
		/// </returns>
		/// <exception cref="T:System.NullReferenceException">
		/// The <paramref name="obj"/> parameter is null.
		/// </exception>
		public override bool Equals(object obj)
		{
			if (obj == null)
				return false;

			var otherValue = obj as RavenJValue;
			return otherValue != null ? Equals(otherValue) : base.Equals(obj);
		}

		/// <summary>
		/// Serves as a hash function for a particular type.
		/// </summary>
		/// <returns>
		/// A hash code for the current <see cref="T:System.Object"/>.
		/// </returns>
		public override int GetHashCode()
		{
			return _value == null ? 0 : _value.GetHashCode();
		}

		int IComparable.CompareTo(object obj)
		{
			if (obj == null)
				return 1;

			var otherValue = (obj is RavenJValue) ? ((RavenJValue)obj).Value : obj;

			return Compare(_valueType, _value, otherValue);
		}

		public bool Equals(RavenJValue other)
		{
			return other != null && ValuesEquals(this, other);
		}

		private static bool ValuesEquals(RavenJValue v1, RavenJValue v2)
		{
			if (v1 == v2)
				return true;

			// HACK: This prevents ValuesEquals from being commutative, need to find a more elegant fix
			// suggestion: if v1._valueType != v2._valueType and one of the value types is a string do a TryParse on the string v2._valueType
			switch (v1._valueType)
			{
				case JTokenType.TimeSpan:
					switch (v2._valueType)
					{
						case JTokenType.TimeSpan:
							break;
						case JTokenType.String:
							TimeSpan val;
							if (TimeSpan.TryParse(v2._value as string, out val))
								return TimeSpan.Equals((TimeSpan)v1._value, val);
							return false;
						default:
							return false;
					}
					break;
				case JTokenType.Guid:
					switch (v2._valueType)
					{
						case JTokenType.String:
						case JTokenType.Guid:
							break;
						default:
							return false;
					}
					break;
				case JTokenType.String:
					switch (v2._valueType)
					{
						case JTokenType.String:
						case JTokenType.Guid:
							break;
						default:
							return false;
					}
					break;
				case JTokenType.Uri:
					switch (v2._valueType)
					{
						case JTokenType.String:
						case JTokenType.Uri:
							break;
						default:
							return false;
					}
					break;
				default:
					if (v1._valueType != v2._valueType)
						return false;
					break;
			}

			return Compare(v1._valueType, v1._value, v2._value) == 0;
		}

		public int CompareTo(RavenJValue other)
		{
			return other == null ? 1 : Compare(_valueType, _value, other._value);
		}

		private static int Compare(JTokenType valueType, object objA, object objB)
		{
			if (objA == null && objB == null)
				return 0;
			if (objA != null && objB == null)
				return 1;
			if (objA == null && objB != null)
				return -1;

			switch (valueType)
			{
				case JTokenType.Integer:
					if (objA is ulong || objB is ulong || objA is decimal || objB is decimal)
						return Convert.ToDecimal(objA, CultureInfo.InvariantCulture).CompareTo(Convert.ToDecimal(objB, CultureInfo.InvariantCulture));
					else if (objA is float || objB is float || objA is double || objB is double)
						return CompareFloat(objA, objB);
					else
						return Convert.ToInt64(objA, CultureInfo.InvariantCulture).CompareTo(Convert.ToInt64(objB, CultureInfo.InvariantCulture));
				case JTokenType.Float:
					return CompareFloat(objA, objB);
				case JTokenType.Comment:
				case JTokenType.String:
				case JTokenType.Raw:
					string s1 = Convert.ToString(objA, CultureInfo.InvariantCulture);
					string s2 = Convert.ToString(objB, CultureInfo.InvariantCulture);

					return string.CompareOrdinal(s1, s2);
				case JTokenType.Boolean:
					bool b1 = Convert.ToBoolean(objA, CultureInfo.InvariantCulture);
					bool b2 = Convert.ToBoolean(objB, CultureInfo.InvariantCulture);

					return b1.CompareTo(b2);
				case JTokenType.Date:
					if (objA is DateTime)
					{
						DateTime date1 = Convert.ToDateTime(objA, CultureInfo.InvariantCulture);
						DateTime date2 = Convert.ToDateTime(objB, CultureInfo.InvariantCulture);

						return date1.CompareTo(date2);
					}
					else
					{
						if (!(objB is DateTimeOffset))
							throw new ArgumentException("Object must be of type DateTimeOffset.");

						DateTimeOffset date1 = (DateTimeOffset)objA;
						DateTimeOffset date2 = (DateTimeOffset)objB;

						return date1.CompareTo(date2);
					}
				case JTokenType.Bytes:
					if (!(objB is byte[]))
						throw new ArgumentException("Object must be of type byte[].");

					byte[] bytes1 = objA as byte[];
					byte[] bytes2 = objB as byte[];
					if (bytes1 == null)
						return -1;
					if (bytes2 == null)
						return 1;

					return MiscellaneousUtils.ByteArrayCompare(bytes1, bytes2);
				case JTokenType.Guid:
					if (!(objB is Guid))
					{
#if !NET35
						Guid guid;
						if (Guid.TryParse((string)objB, out guid) == false)
							throw new ArgumentException("Object must be of type Guid.");
						objB = guid;
#else
						objB = new Guid((string)objB);
#endif
					}

					Guid guid1 = (Guid)objA;
					Guid guid2 = (Guid)objB;

					return guid1.CompareTo(guid2);
				case JTokenType.Uri:
					if (objB is string)
						objB = new Uri((string)objB);

					if (!(objB is Uri))
						throw new ArgumentException("Object must be of type Uri.");

					Uri uri1 = (Uri)objA;
					Uri uri2 = (Uri)objB;

					return Comparer<string>.Default.Compare(uri1.ToString(), uri2.ToString());
				case JTokenType.TimeSpan:
					if (!(objB is TimeSpan))
						throw new ArgumentException("Object must be of type TimeSpan.");

					TimeSpan ts1 = (TimeSpan)objA;
					TimeSpan ts2 = (TimeSpan)objB;

					return ts1.CompareTo(ts2);
				default:
					throw MiscellaneousUtils.CreateArgumentOutOfRangeException("valueType", valueType, "Unexpected value type: {0}".FormatWith(CultureInfo.InvariantCulture, valueType));
			}
		}

		private static int CompareFloat(object objA, object objB)
		{
			double d1 = Convert.ToDouble(objA, CultureInfo.InvariantCulture);
			double d2 = Convert.ToDouble(objB, CultureInfo.InvariantCulture);

			// take into account possible floating point errors
			if (MathUtils.ApproxEquals(d1, d2))
				return 0;

			return d1.CompareTo(d2);
		}

		internal override bool DeepEquals(RavenJToken node)
		{
			var other = node as RavenJValue;
			if (other == null)
				return false;
			if (other == this)
				return true;

			return ValuesEquals(this, other);
		}

		internal override int GetDeepHashCode()
		{
			int valueHashCode = (_value != null) ? _value.GetHashCode() : 0;

			return _valueType.GetHashCode() ^ valueHashCode;
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			if (_value == null)
				return string.Empty;

			return _value.ToString();
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <param name="format">The format.</param>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public string ToString(string format)
		{
			return ToString(format, CultureInfo.CurrentCulture);
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <param name="formatProvider">The format provider.</param>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public string ToString(IFormatProvider formatProvider)
		{
			return ToString(null, formatProvider);
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <param name="format">The format.</param>
		/// <param name="formatProvider">The format provider.</param>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public string ToString(string format, IFormatProvider formatProvider)
		{
			if (_value == null)
				return string.Empty;

			var formattable = _value as IFormattable;
			if (formattable != null)
				return formattable.ToString(format, formatProvider);

			return _value.ToString();
		}

		public override void EnsureSnapshot()
		{
			isSnapshot = true;
		}

		public override RavenJToken CreateSnapshot()
		{
			if (!isSnapshot)
				throw new InvalidOperationException("Cannot create snapshot without previously calling EnsureSnapShot");

			return new RavenJValue(Value);
		}

		public static RavenJValue Null
		{
			get { return new RavenJValue(null, JTokenType.Null); }
		}
	}
}