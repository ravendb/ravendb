using System;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
		/// <returns>
		/// A <see cref="T:Byte[]"/> or a null reference if the next JSON token is null.
		/// </returns>
		public override byte[] ReadAsBytes()
		{
			Read();

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

			if (ReaderIsSerializerInArray())
				return null;

			throw CreateReaderException(this, "Error reading bytes. Expected bytes but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

		private bool IsWrappedInTypeObject()
		{
			if (TokenType == JsonToken.StartObject)
			{
				Read();
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
		/// <returns>A <see cref="Nullable{Decimal}"/>.</returns>
		public override decimal? ReadAsDecimal()
		{
			Read();

			if (TokenType == JsonToken.Integer || TokenType == JsonToken.Float)
			{
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

			if (ReaderIsSerializerInArray())
				return null;

			throw CreateReaderException(this, "Error reading decimal. Expected a number but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{Int32}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{Int32}"/>.</returns>
		public override int? ReadAsInt32()
		{
			Read();

			if (TokenType == JsonToken.Integer || TokenType == JsonToken.Float)
			{
				SetToken(JsonToken.Integer, Convert.ToInt32(Value, CultureInfo.InvariantCulture));
				return (int)Value;
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

			if (ReaderIsSerializerInArray())
				return null;

			throw CreateReaderException(this, "Error reading integer. Expected a number but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

#if !NET20
		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{DateTimeOffset}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{DateTimeOffset}"/>.</returns>
		public override DateTimeOffset? ReadAsDateTimeOffset()
		{
			Read();

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

			if (ReaderIsSerializerInArray())
				return null;

			throw CreateReaderException(this, "Error reading date. Expected bytes but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
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

		internal bool ReaderIsSerializerInArray()
		{
			return TokenType == JsonToken.EndArray;
		}

		private JsonReaderException CreateReaderException(JsonReader reader, string message)
		{
			return new JsonReaderException(message);
		}
	}
}