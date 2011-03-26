using System;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
    public class RavenJObject : RavenJToken
    {
        /// <summary>
        /// Gets the node type for this <see cref="RavenJToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public override JTokenType Type
        {
            get { return JTokenType.Object; }
        }

        public IDictionary<string, RavenJToken> Properties
        {
            get { return _properties ?? (_properties = new CopyOnWriteJDictionary<string>()); }
        }
        private CopyOnWriteJDictionary<string> _properties;

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJObject"/> class.
        /// </summary>
        public RavenJObject()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJObject"/> class from another <see cref="RavenJObject"/> object.
        /// </summary>
        /// <param name="other">A <see cref="RavenJObject"/> object to copy from.</param>
        public RavenJObject(RavenJObject other)
        {
            _properties = (CopyOnWriteJDictionary<string>) other._properties.Clone();
        }

        public override RavenJToken CloneToken()
        {
            return new RavenJObject(this);
        }

        /// <summary>
        /// Creates a <see cref="RavenJObject"/> from an object.
        /// </summary>
		/// <param name="o">The object that will be used to create <see cref="RavenJObject"/>.</param>
		/// <returns>A <see cref="RavenJObject"/> with the values of the specified object</returns>
        public static new RavenJObject FromObject(object o)
        {
            return FromObject(o, new JsonSerializer());
        }

        /// <summary>
        /// Creates a <see cref="RavenJArray"/> from an object.
        /// </summary>
        /// <param name="o">The object that will be used to create <see cref="RavenJArray"/>.</param>
        /// <param name="jsonSerializer">The <see cref="JsonSerializer"/> that will be used to read the object.</param>
        /// <returns>A <see cref="RavenJArray"/> with the values of the specified object</returns>
        public static new RavenJObject FromObject(object o, JsonSerializer jsonSerializer)
        {
            RavenJToken token = FromObjectInternal(o, jsonSerializer);

            if (token != null && token.Type != JTokenType.Object)
                throw new ArgumentException("Object serialized to {0}. JObject instance expected.".FormatWith(CultureInfo.InvariantCulture, token.Type));

            return (RavenJObject)token;
        }

        /// <summary>
        /// Gets or sets the <see cref="RavenJToken"/> with the specified property name.
        /// </summary>
        /// <value></value>
        public RavenJToken this[string propertyName]
        {
            get
            {
                ValidationUtils.ArgumentNotNull(propertyName, "propertyName");
                return Properties[propertyName];
            }
            set { Properties[propertyName] = value; }
        }

        /// <summary>
        /// Loads an <see cref="RavenJObject"/> from a <see cref="JsonReader"/>. 
        /// </summary>
        /// <param name="reader">A <see cref="JsonReader"/> that will be read for the content of the <see cref="RavenJObject"/>.</param>
        /// <returns>A <see cref="RavenJObject"/> that contains the JSON that was read from the specified <see cref="JsonReader"/>.</returns>
        public static RavenJObject Load(JsonReader reader)
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
            var o = new RavenJObject();
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
