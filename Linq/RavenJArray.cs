using System;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
    public class RavenJArray : RavenJToken
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JArray"/> class.
        /// </summary>
        public RavenJArray()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JArray"/> class from another <see cref="JArray"/> object.
        /// </summary>
        /// <param name="other">A <see cref="JArray"/> object to copy from.</param>
        public RavenJArray(RavenJArray other)
        {
            if (other.Length == 0) return;

            // clone array the hard way
            foreach (var item in other._items)
                Items.Add(item.CloneToken());
        }

        /// <summary>
        /// Gets the node type for this <see cref="JToken"/>.
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

        internal static RavenJArray Load(JsonReader reader)
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
    }
}
