using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;
using System.Collections;

namespace Raven.Json.Linq
{
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
			_root = token;
		}

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
					foreach (var item in ((RavenJArray)token).Items)
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

		public override byte[] ReadAsBytes()
		{
			Read();

			// attempt to convert possible base 64 string to bytes
			if (TokenType == JsonToken.String)
			{
				var s = (string) Value;
				var data = (s.Length == 0) ? new byte[0] : Convert.FromBase64String(s);
				SetToken(JsonToken.Bytes, data);
			}

			if (TokenType == JsonToken.Null)
				return null;
			if (TokenType == JsonToken.Bytes)
				return (byte[]) Value;

			throw new JsonReaderException(
				"Error reading bytes. Expected bytes but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

		public override decimal? ReadAsDecimal()
		{
			Read();

			if (TokenType == JsonToken.Null)
				return null;
			if (TokenType == JsonToken.Integer || TokenType == JsonToken.Float)
			{
				SetToken(JsonToken.Float, Convert.ToDecimal(Value, CultureInfo.InvariantCulture));
				return (decimal) Value;
			}

			throw new JsonReaderException(
				"Error reading decimal. Expected a number but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}

#if !NET20
		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{DateTimeOffset}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{DateTimeOffset}"/>.</returns>
		public override DateTimeOffset? ReadAsDateTimeOffset()
		{
			Read();

			if (TokenType == JsonToken.Null)
				return null;
			if (TokenType == JsonToken.Date)
			{
				SetToken(JsonToken.Date, new DateTimeOffset((DateTime) Value));
				return (DateTimeOffset) Value;
			}

			throw new JsonReaderException(
				"Error reading date. Expected bytes but got {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType));
		}
#endif
	}
}
