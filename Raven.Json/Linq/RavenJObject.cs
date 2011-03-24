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
    public class RavenJObject : RavenJToken
    {
        /// <summary>
        /// Gets the node type for this <see cref="JToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public override JTokenType Type
        {
            get { return JTokenType.Object; }
        }

        public Dictionary<string, RavenJToken> Properties
        {
            get
            {
                if (_properties == null)
                    _properties = new Dictionary<string, RavenJToken>();
                return _properties;
            }
        }
        private Dictionary<string, RavenJToken> _properties;

        /// <summary>
        /// Initializes a new instance of the <see cref="JObject"/> class.
        /// </summary>
        public RavenJObject()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JObject"/> class from another <see cref="JObject"/> object.
        /// </summary>
        /// <param name="other">A <see cref="JObject"/> object to copy from.</param>
        public RavenJObject(RavenJObject other)
        {
            var en = other.Properties.GetEnumerator();
            while (en.MoveNext())
            {
                Properties.Add(en.Current.Key, en.Current.Value.DeepClone());
            }
        }

        internal override RavenJToken CloneToken()
        {
            return new RavenJObject(this);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JObject"/> class with the specified content.
        /// </summary>
        /// <param name="propName">Propery name for this content</param>
        /// <param name="content">The contents of the object.</param>
        public RavenJObject(string propName, RavenJToken content)
        {
            Properties.Add(propName, content);
        }

        /// <summary>
        /// Loads an <see cref="RavenJObject"/> from a <see cref="JsonReader"/>. 
        /// </summary>
        /// <param name="reader">A <see cref="JsonReader"/> that will be read for the content of the <see cref="RavenJObject"/>.</param>
        /// <returns>A <see cref="RavenJObject"/> that contains the JSON that was read from the specified <see cref="JsonReader"/>.</returns>
        public static new RavenJObject Load(JsonReader reader)
        {
            ValidationUtils.ArgumentNotNull(reader, "reader");

            if (reader.TokenType == JsonToken.None)
            {
                if (!reader.Read())
                    throw new Exception("Error reading JObject from JsonReader.");
            }

            if (reader.TokenType != JsonToken.StartObject)
                throw new Exception(
                    "Error reading JObject from JsonReader. Current JsonReader item is not an object: {0}".FormatWith(
                        CultureInfo.InvariantCulture, reader.TokenType));

            if (reader.Read() == false)
                throw new Exception("Unexpected end of json object");

            string propName = null;
            RavenJObject o = new RavenJObject();
            do
            {
                switch (reader.TokenType)
                {
                    case JsonToken.Comment:
                        // ignore comments
                        break;
                    case JsonToken.PropertyName:
                        propName = reader.Value.ToString();
                        break;
                    case JsonToken.EndObject:
                        return o;
                    case JsonToken.StartObject:
                        if (!string.IsNullOrEmpty(propName))
                        {
                            var val = RavenJObject.Load(reader);
                            o.Properties.Add(propName, val);
                            propName = null;
                        }
                        else
                        {
                            throw new InvalidOperationException("The JsonReader should not be on a token of type {0}."
                                                                    .FormatWith(CultureInfo.InvariantCulture,
                                                                                reader.TokenType));
                        }
                        break;
                    case JsonToken.StartArray:
                        if (!string.IsNullOrEmpty(propName))
                        {
                            var val = RavenJArray.Load(reader);
                            o.Properties.Add(propName, val);
                            propName = null;
                        }
                        else
                        {
                            throw new InvalidOperationException("The JsonReader should not be on a token of type {0}."
                                                                    .FormatWith(CultureInfo.InvariantCulture,
                                                                                reader.TokenType));
                        }
                        break;
                    default:
                        if (!string.IsNullOrEmpty(propName))
                        {
                            var val = RavenJValue.Load(reader);
                            o.Properties.Add(propName, val);
                            propName = null;
                        }
                        else
                        {
                            throw new InvalidOperationException("The JsonReader should not be on a token of type {0}."
                                                                    .FormatWith(CultureInfo.InvariantCulture,
                                                                                reader.TokenType));
                        }
                        break;
                }
            } while (reader.Read());

            throw new Exception("Error reading JObject from JsonReader.");
        }
    }
}
