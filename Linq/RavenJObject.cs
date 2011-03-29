using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
    public class RavenJObject : RavenJToken, IEnumerable<KeyValuePair<string, RavenJToken>>
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

		public override IEnumerable<RavenJToken> Children()
		{
			return Properties.Values;
		}

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJObject"/> class.
        /// </summary>
        public RavenJObject()
        {
        }

		public RavenJObject(params KeyValuePair<string, RavenJToken>[] props)
		{
			_properties = new CopyOnWriteJDictionary<string>();
			foreach (var kv in props)
			{
				_properties.Add(kv);
			}
		}

		public RavenJObject(object o)
		{
			if (o is KeyValuePair<string, RavenJToken>)
			{
				_properties = new CopyOnWriteJDictionary<string>();
				_properties.Add((KeyValuePair<string, RavenJToken>)o);
			}
		}

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenJObject"/> class from another <see cref="RavenJObject"/> object.
        /// </summary>
        /// <param name="other">A <see cref="RavenJObject"/> object to copy from.</param>
        public RavenJObject(RavenJObject other)
        {
            _properties = (CopyOnWriteJDictionary<string>) other._properties.Clone();
        }

		/// <summary>
		/// Gets the <see cref="JToken"/> with the specified key.
		/// </summary>
		/// <value>The <see cref="JToken"/> with the specified key.</value>
		public override RavenJToken this[object key]
		{
			get
			{
				ValidationUtils.ArgumentNotNull(key, "o");

				var propertyName = key as string;
				if (propertyName == null)
					throw new ArgumentException("Accessed RavenJObject values with invalid key value: {0}. Object property name expected.".FormatWith(CultureInfo.InvariantCulture, MiscellaneousUtils.ToString(key)));

				return this[propertyName];
			}
			set
			{
				ValidationUtils.ArgumentNotNull(key, "o");

				var propertyName = key as string;
				if (propertyName == null)
					throw new ArgumentException("Set RavenJObject values with invalid key value: {0}. Object property name expected.".FormatWith(CultureInfo.InvariantCulture, MiscellaneousUtils.ToString(key)));

				this[propertyName] = value;
			}
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

        public override RavenJToken CloneToken()
        {
            return new RavenJObject(this);
        }

		public void AddValueProperty(string key, object value)
		{
			Properties.Add(key, new RavenJValue(value));
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
                throw new ArgumentException("Object serialized to {0}. RavenJObject instance expected.".FormatWith(CultureInfo.InvariantCulture, token.Type));

            return (RavenJObject)token;
        }

        /// <summary>
        /// Loads an <see cref="RavenJObject"/> from a <see cref="JsonReader"/>. 
        /// </summary>
        /// <param name="reader">A <see cref="JsonReader"/> that will be read for the content of the <see cref="RavenJObject"/>.</param>
        /// <returns>A <see cref="RavenJObject"/> that contains the JSON that was read from the specified <see cref="JsonReader"/>.</returns>
        public new static RavenJObject Load(JsonReader reader)
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

		/// <summary>
		/// Load a <see cref="RavenJObject"/> from a string that contains JSON.
		/// </summary>
		/// <param name="json">A <see cref="String"/> that contains JSON.</param>
		/// <returns>A <see cref="RavenJObject"/> populated from the string that contains JSON.</returns>
		public static new RavenJObject Parse(string json)
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
			writer.WriteStartObject();

			if (_properties != null)
			{
				foreach (var property in _properties)
				{
					writer.WritePropertyName(property.Key);
					property.Value.WriteTo(writer, converters);
				}
			}

			writer.WriteEndObject();
		}

		internal override bool DeepEquals(RavenJToken node)
		{
			var t = node as RavenJObject;
			if (t == null)
				return false;

			if (_properties == null || t._properties == null)
			{
				if (_properties == t._properties)
					return true;
				return false;
			}

			RavenJToken v1, v2;
			foreach (var key in _properties.Keys)
			{
				if (!t._properties.TryGetValue(key, out v2) || !_properties.TryGetValue(key, out v1))
					return false;

				if (v1 == null || v2 == null)
				{
					if (v1 == v2)
						continue;

					return false;
				}

				if (!v1.DeepEquals(v2))
					return false;
			}
			return true;
		}

		internal override int GetDeepHashCode()
		{
			int hashCode = 0;
			if (_properties != null)
			{
				foreach (RavenJToken item in _properties.Values)
				{
					hashCode ^= item.GetDeepHashCode();
				}
			}
			return hashCode;
		}

		#region IEnumerable<KeyValuePair<string,RavenJToken>> Members

		public IEnumerator<KeyValuePair<string, RavenJToken>> GetEnumerator()
		{
			return Properties.GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		public void Add(string propName, RavenJToken token)
		{
			Properties.Add(propName, token);
		}
	}
}
