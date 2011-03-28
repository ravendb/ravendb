using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
    public class RavenJArray : RavenJToken, IEnumerable<RavenJToken>
    {
        /// <summary>
		/// Initializes a new instance of the <see cref="RavenJArray"/> class.
        /// </summary>
        public RavenJArray()
        {
        }

        /// <summary>
		/// Initializes a new instance of the <see cref="RavenJArray"/> class from another <see cref="RavenJArray"/> object.
        /// </summary>
		/// <param name="other">A <see cref="RavenJArray"/> object to copy from.</param>
        public RavenJArray(RavenJArray other)
        {
            if (other.Length == 0) return;

            // clone array the hard way
            foreach (var item in other._items)
                Items.Add(item.CloneToken());
        }

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenJArray"/> class with the specified content.
		/// </summary>
		/// <param name="content">The contents of the array.</param>
		public RavenJArray(params object[] content)
		{
			_items = new List<RavenJToken>();
			foreach (var item in content)
			{
				if (item.GetType().IsSubclassOf(typeof(RavenJToken)))
					_items.Add((RavenJToken)item);
				else
					_items.Add(new RavenJValue(item));
			}
		}

    	/// <summary>
        /// Gets the node type for this <see cref="RavenJToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public override JTokenType Type
        {
            get { return JTokenType.Array; }
        }

        public override RavenJToken CloneToken()
        {
            return new RavenJArray(this);
        }

        public int Length { get { return _items == null ? 0 : _items.Count; } }

        public List<RavenJToken> Items
        {
            get { return _items ?? (_items = new List<RavenJToken>()); }
        }
        private List<RavenJToken> _items;

        internal new static RavenJArray Load(JsonReader reader)
        {
            if (reader.TokenType != JsonToken.StartArray)
                throw new Exception(
                    "Error reading JObject from JsonReader. Current JsonReader item is not an object: {0}".FormatWith(
                        CultureInfo.InvariantCulture, reader.TokenType));

            if (reader.Read() == false)
                throw new Exception("Unexpected end of json array");

            var ar = new RavenJArray();
            RavenJToken val = null;
            do
            {
                switch (reader.TokenType)
                {
                    case JsonToken.Comment:
                        // ignore comments
                        break;
                    case JsonToken.EndArray:
                        return ar;
                    case JsonToken.StartObject:
                        val = RavenJObject.Load(reader);
                        ar.Items.Add(val);
                        break;
                    case JsonToken.StartArray:
                        val = RavenJArray.Load(reader);
                        ar.Items.Add(val);
                        break;
                    default:
                        val = RavenJValue.Load(reader);
                        ar.Items.Add(val);
                        break;
                }
            } while (reader.Read());

            throw new Exception("Error reading JArray from JsonReader.");
        }

		/// <summary>
		/// Load a <see cref="RavenJArray"/> from a string that contains JSON.
		/// </summary>
		/// <param name="json">A <see cref="String"/> that contains JSON.</param>
		/// <returns>A <see cref="RavenJArray"/> populated from the string that contains JSON.</returns>
		public static new RavenJArray Parse(string json)
		{
			JsonReader jsonReader = new JsonTextReader(new StringReader(json));

			return Load(jsonReader);
		}

		/// <summary>
		/// Writes this token to a <see cref="JsonWriter"/>.
		/// </summary>
		/// <param name="writer">A <see cref="JsonWriter"/> into which this method will write.</param>
		/// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
		public override void WriteTo(JsonWriter writer, params JsonConverter[] converters)
		{
			writer.WriteStartArray();

			if (_items != null)
			{
				foreach (var token in _items)
				{
					token.WriteTo(writer, converters);
				}
			}

			writer.WriteEndArray();
		}

		#region IEnumerable<RavenJToken> Members

		public IEnumerator<RavenJToken> GetEnumerator()
		{
			return Items.GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion
	}
}
