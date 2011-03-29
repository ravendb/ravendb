using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
	public class RavenJTokenReader : JsonReader
	{
		private readonly RavenJToken _root;

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
			return ReadRavenJToken(_root);
		}

		private bool ReadRavenJToken(RavenJToken token)
		{
			if (token is RavenJValue)
			{
				SetToken(token);
			}
			else if (token is RavenJArray)
			{
				SetToken(JsonToken.StartArray);
				if (((RavenJArray)token).Length > 0) // to prevent object creation if inner array is null
				{
					foreach (var item in ((RavenJArray) token).Items)
					{
						if (!ReadRavenJToken(item))
							return false;
					}
				}
				SetToken(JsonToken.EndObject);
			}
			else if (token is RavenJObject)
			{
				SetToken(JsonToken.StartObject);

				foreach (var prop in ((RavenJObject)token).Properties)
				{
					SetToken(JsonToken.PropertyName, prop.Key);
					if (!ReadRavenJToken(prop.Value))
						return false;
				}

				SetToken(JsonToken.EndObject);
			}
			return true;
		}

		private void SetToken(RavenJToken token)
		{
			switch (token.Type)
			{
				case JTokenType.Integer:
					SetToken(JsonToken.Integer, ((RavenJValue)token).Value);
					break;
				case JTokenType.Float:
					SetToken(JsonToken.Float, ((RavenJValue)token).Value);
					break;
				case JTokenType.String:
					SetToken(JsonToken.String, ((RavenJValue)token).Value);
					break;
				case JTokenType.Boolean:
					SetToken(JsonToken.Boolean, ((RavenJValue)token).Value);
					break;
				case JTokenType.Null:
					SetToken(JsonToken.Null, ((RavenJValue)token).Value);
					break;
				case JTokenType.Undefined:
					SetToken(JsonToken.Undefined, ((RavenJValue)token).Value);
					break;
				case JTokenType.Date:
					SetToken(JsonToken.Date, ((RavenJValue)token).Value);
					break;
				case JTokenType.Raw:
					SetToken(JsonToken.Raw, ((RavenJValue)token).Value);
					break;
				case JTokenType.Bytes:
					SetToken(JsonToken.Bytes, ((RavenJValue)token).Value);
					break;
				default:
					throw MiscellaneousUtils.CreateArgumentOutOfRangeException("Type", token.Type, "Unexpected JTokenType.");
			}
		}

		public override byte[] ReadAsBytes()
		{
			throw new NotImplementedException();
		}

		public override decimal? ReadAsDecimal()
		{
			throw new NotImplementedException();
		}

		public override DateTimeOffset? ReadAsDateTimeOffset()
		{
			throw new NotImplementedException();
		}
	}
}
