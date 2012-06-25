using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
	/// <summary>
	/// Represents a reader that provides fast, non-cached, forward-only access to serialized Json data.
	/// </summary>
	public class RavenJTokenReader : JsonReader
	{
		private readonly RavenJToken _root;
		private IEnumerator<ReadState> enumerator;

		private class ReadState
		{
			public ReadState(JsonToken type, object val = null)
			{
				TokenType = type;
				Value = val;
			}
			public JsonToken TokenType { get; private set; }
			public object Value { get; private set; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenJTokenReader"/> class.
		/// </summary>
		/// <param name="token">The token to read from.</param>
		public RavenJTokenReader(RavenJToken token)
		{
			if (token == null)
				throw new ArgumentNullException("token");

			_root = token;
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="T:Byte[]"/>.
		/// </summary>
		/// <returns>A <see cref="T:Byte[]"/> or a null reference if the next JSON token is null. This method will return <c>null</c> at the end of an array.</returns>
		public override byte[] ReadAsBytes()
		{
			if (!Read())
			{
				SetToken(JsonToken.None);
				return null;
			}

			if (IsWrappedInTypeObject())
			{
				byte[] data = ReadAsBytes();
				Read();
				SetToken(JsonToken.Bytes, data);
				return data;
			}

			// attempt to convert possible base 64 string to bytes
			if (TokenType == JsonToken.String)
			{
				var s = (string)Value;
				byte[] data = (s.Length == 0) ? new byte[0] : Convert.FromBase64String(s);
				SetToken(JsonToken.Bytes, data);
			}

			if (TokenType == JsonToken.Null)
				return null;
			if (TokenType == JsonToken.Bytes)
				return (byte[])Value;

			if (TokenType == JsonToken.StartArray)
			{
				List<byte> data = new List<byte>();

				while (Read())
				{
					switch (TokenType)
					{
						case JsonToken.Integer:
							data.Add(Convert.ToByte(Value, CultureInfo.InvariantCulture));
							break;
						case JsonToken.EndArray:
							byte[] d = data.ToArray();
							SetToken(JsonToken.Bytes, d);
							return d;
						case JsonToken.Comment:
							// skip
							break;
						default:
							throw CreateReaderException(this, "Unexpected token when reading bytes: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
					}
				}

				throw CreateReaderException(this, "Unexpected end when reading bytes.");
			}

			if (TokenType == JsonToken.EndArray)
				return null;

			throw CreateReaderException(this, "Error reading bytes. Expected bytes but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

		private bool IsWrappedInTypeObject()
		{
			if (TokenType == JsonToken.StartObject)
			{
				if (!Read())
					throw CreateReaderException(this, "Unexpected end when reading bytes.");

				if (Value.ToString() == "$type")
				{
					Read();
					if (Value != null && Value.ToString().StartsWith("System.Byte[]"))
					{
						Read();
						if (Value.ToString() == "$value")
						{
							return true;
						}
					}
				}

				throw CreateReaderException(this, "Unexpected token when reading bytes: {0}.".FormatWith(CultureInfo.InvariantCulture, JsonToken.StartObject));
			}

			return false;
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{Decimal}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{Decimal}"/>. This method will return <c>null</c> at the end of an array.</returns>
		public override decimal? ReadAsDecimal()
		{
			if (!Read())
			{
				SetToken(JsonToken.None);
				return null;
			}

			if (TokenType == JsonToken.Integer || TokenType == JsonToken.Float)
			{
				if (Value is decimal == false)
					SetToken(JsonToken.Float, Convert.ToDecimal(Value, CultureInfo.InvariantCulture));

				return (decimal)Value;
			}

			if (TokenType == JsonToken.Null)
				return null;

			decimal d;
			if (TokenType == JsonToken.String)
			{
				if (decimal.TryParse((string)Value, NumberStyles.Number, Culture, out d))
				{
					SetToken(JsonToken.Float, d);
					return d;
				}
				else
				{
					throw CreateReaderException(this, "Could not convert string to decimal: {0}.".FormatWith(CultureInfo.InvariantCulture, Value));
				}
			}

			if (TokenType == JsonToken.EndArray)
				return null;

			throw CreateReaderException(this, "Error reading decimal. Expected a number but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{Int32}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{Int32}"/>. This method will return <c>null</c> at the end of an array.</returns>
		public override int? ReadAsInt32()
		{
			if (!Read())
			{
				SetToken(JsonToken.None);
				return null;
			}

			if (TokenType == JsonToken.Integer || TokenType == JsonToken.Float)
			{
				if (Value is int == false)
					SetToken(JsonToken.Integer, Convert.ToInt32(Value, CultureInfo.InvariantCulture));

				return (int) Value;
			}

			if (TokenType == JsonToken.Null)
				return null;

			int i;
			if (TokenType == JsonToken.String)
			{
				if (int.TryParse((string)Value, NumberStyles.Integer, Culture, out i))
				{
					SetToken(JsonToken.Integer, i);
					return i;
				}
				else
				{
					throw CreateReaderException(this, "Could not convert string to integer: {0}.".FormatWith(CultureInfo.InvariantCulture, Value));
				}
			}

			if (TokenType == JsonToken.EndArray)
				return null;

			throw CreateReaderException(this, "Error reading integer. Expected a number but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="String"/>.
		/// </summary>
		/// <returns>A <see cref="String"/>. This method will return <c>null</c> at the end of an array.</returns>
		public override string ReadAsString()
		{
			if (!Read())
			{
				SetToken(JsonToken.None);
				return null;
			}

			if (TokenType == JsonToken.String)
				return (string)Value;

			if (TokenType == JsonToken.Null)
				return null;

			if (IsPrimitiveToken(TokenType))
			{
				if (Value != null)
				{
					string s;
					if (Value is IConvertible)
						s = ((IConvertible)Value).ToString(Culture);
					else if (Value is IFormattable)
						s = ((IFormattable)Value).ToString(null, Culture);
					else
						s = Value.ToString();

					SetToken(JsonToken.String, s);
					return s;
				}
			}

			if (TokenType == JsonToken.EndArray)
				return null;

			throw CreateReaderException(this, "Error reading string. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{DateTime}"/>.
		/// </summary>
		/// <returns>A <see cref="String"/>. This method will return <c>null</c> at the end of an array.</returns>
		public override DateTime? ReadAsDateTime()
		{
			if (!Read())
			{
				SetToken(JsonToken.None);
				return null;
			}

			if (TokenType == JsonToken.Date)
				return (DateTime)Value;

			if (TokenType == JsonToken.Null)
				return null;

			DateTime dt;
			if (TokenType == JsonToken.String)
			{
				string s = (string)Value;
				if (string.IsNullOrEmpty(s))
				{
					SetToken(JsonToken.Null);
					return null;
				}

				if (DateTime.TryParse(s, Culture, DateTimeStyles.RoundtripKind, out dt))
				{
					dt = RavenJsonConvert.EnsureDateTime(dt, DateTimeZoneHandling);
					SetToken(JsonToken.Date, dt);
					return dt;
				}
				else
				{
					throw CreateReaderException(this, "Could not convert string to DateTime: {0}.".FormatWith(CultureInfo.InvariantCulture, Value));
				}
			}

			if (TokenType == JsonToken.EndArray)
				return null;

			throw CreateReaderException(this, "Error reading date. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

#if !NET20
		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{DateTimeOffset}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{DateTimeOffset}"/>. This method will return <c>null</c> at the end of an array.</returns>
		public override DateTimeOffset? ReadAsDateTimeOffset()
		{
			if (!Read())
			{
				SetToken(JsonToken.None);
				return null;
			}

			if (TokenType == JsonToken.Date)
			{
				SetToken(JsonToken.Date, new DateTimeOffset((DateTime)Value));
				return (DateTimeOffset)Value;
			}

			if (TokenType == JsonToken.Null)
				return null;

			DateTimeOffset dt;
			if (TokenType == JsonToken.String)
			{
				if (DateTimeOffset.TryParse((string)Value, Culture, DateTimeStyles.None, out dt))
				{
					SetToken(JsonToken.Date, dt);
					return dt;
				}
				else
				{
					throw CreateReaderException(this, "Could not convert string to DateTimeOffset: {0}.".FormatWith(CultureInfo.InvariantCulture, Value));
				}
			}

			if (TokenType == JsonToken.EndArray)
				return null;

			throw CreateReaderException(this, "Error reading date. Expected date but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}
#endif

		/// <summary>
		/// Reads the next JSON token from the stream.
		/// </summary>
		/// <returns>
		/// true if the next token was read successfully; false if there are no more tokens to read.
		/// </returns>
		public override bool Read()
		{
			if (CurrentState == State.Start)
				enumerator = ReadRavenJToken(_root).GetEnumerator();

			if (!enumerator.MoveNext())
				return false;

			SetToken(enumerator.Current.TokenType, enumerator.Current.Value);
			return true;
		}

		private static IEnumerable<ReadState> ReadRavenJToken(RavenJToken token)
		{
			if (token is RavenJValue)
			{
				yield return new ReadState(GetJsonTokenType(token), ((RavenJValue)token).Value);
			}
			else if (token is RavenJArray)
			{
				yield return new ReadState(JsonToken.StartArray);
				if (((RavenJArray)token).Length > 0) // to prevent object creation if inner array is null
				{
					foreach (var item in ((RavenJArray)token))
						foreach (var i in ReadRavenJToken(item))
							yield return i;
				}
				yield return new ReadState(JsonToken.EndArray);
			}
			else if (token is RavenJObject)
			{
				yield return new ReadState(JsonToken.StartObject);

				foreach (var prop in ((RavenJObject)token))
				{
					yield return new ReadState(JsonToken.PropertyName, prop.Key);
					foreach (var item in ReadRavenJToken(prop.Value))
						yield return item;
				}

				yield return new ReadState(JsonToken.EndObject);
			}
		}

		private static JsonToken GetJsonTokenType(RavenJToken token)
		{
			switch (token.Type)
			{
				case JTokenType.Integer:
					return JsonToken.Integer;
				case JTokenType.Float:
					return JsonToken.Float;
				case JTokenType.String:
					return JsonToken.String;
				case JTokenType.Boolean:
					return JsonToken.Boolean;
				case JTokenType.Null:
					return JsonToken.Null;
				case JTokenType.Undefined:
					return JsonToken.Undefined;
				case JTokenType.Date:
					return JsonToken.Date;
				case JTokenType.Raw:
					return JsonToken.Raw;
				case JTokenType.Bytes:
					return JsonToken.Bytes;
				default:
					throw MiscellaneousUtils.CreateArgumentOutOfRangeException("Type", token.Type, "Unexpected JTokenType.");
			}
		}

		private static JsonReaderException CreateReaderException(JsonReader reader, string message)
		{
			return new JsonReaderException(message);
		}

		internal new static bool IsPrimitiveToken(JsonToken token)
		{
			switch (token)
			{
				case JsonToken.Integer:
				case JsonToken.Float:
				case JsonToken.String:
				case JsonToken.Boolean:
				case JsonToken.Undefined:
				case JsonToken.Null:
				case JsonToken.Date:
				case JsonToken.Bytes:
					return true;
				default:
					return false;
			}
		}
	}
}