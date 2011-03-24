using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
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
            if (other.Length > 0)
            {
                // clone array the hard way
                foreach (var item in other._items)
                    this.Items.Add(item.DeepClone());
            }
        }

        /// <summary>
        /// Gets the node type for this <see cref="JToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public override JTokenType Type
        {
            get { return JTokenType.Array; }
        }

        internal override RavenJToken CloneToken()
        {
            return new RavenJArray(this);
        }

        public int Length { get { if (_items == null) return 0; return _items.Count; } }

        public List<RavenJToken> Items
        {
            get
            {
                if (_items == null)
                    _items = new List<RavenJToken>();
                return _items;
            }
        }
        private List<RavenJToken> _items;

        internal static new RavenJArray Load(JsonReader reader)
        {
            if (reader.TokenType != JsonToken.StartArray)
                throw new Exception(
                    "Error reading JObject from JsonReader. Current JsonReader item is not an object: {0}".FormatWith(
                        CultureInfo.InvariantCulture, reader.TokenType));

            if (reader.Read() == false)
                throw new Exception("Unexpected end of json array");

            RavenJToken val = null;
            RavenJArray ar = new RavenJArray();
            do
            {
                switch (reader.TokenType)
                {
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

            return null;
        }
    }
}
