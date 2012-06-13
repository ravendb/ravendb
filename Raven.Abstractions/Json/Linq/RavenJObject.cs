using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
	/// <summary>
	/// Represents a JSON object.
	/// </summary>
	public class RavenJObject : RavenJToken, IEnumerable<KeyValuePair<string, RavenJToken>>
	{
		private readonly IEqualityComparer<string> comparer;

		/// <summary>
		/// Gets the node type for this <see cref="RavenJToken"/>.
		/// </summary>
		/// <value>The type.</value>
		public override JTokenType Type
		{
			get { return JTokenType.Object; }
		}

		internal DictionaryWithParentSnapshot Properties { get; set; }

		public int Count
		{
			get { return Properties.Count; }
		}

		public ICollection<string> Keys
		{
			get { return Properties.Keys; }
		}

		public RavenJObject WithCaseInsensitivePropertyNames()
		{
			var props = new DictionaryWithParentSnapshot(StringComparer.InvariantCultureIgnoreCase);
			foreach (var property in Properties)
			{
				props[property.Key] = property.Value;
			}
			return new RavenJObject(props);
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenJObject"/> class.
		/// </summary>
		public RavenJObject() :this(StringComparer.InvariantCulture)
		{
		}

		public RavenJObject(IEqualityComparer<string> comparer)
		{
			this.comparer = comparer;
			Properties = new DictionaryWithParentSnapshot(comparer);
		}

		public RavenJObject(RavenJObject other)
		{
			Properties = new DictionaryWithParentSnapshot(other.comparer);
			foreach (var kv in other.Properties)
			{
				Properties.Add(kv);
			}
		}

		private RavenJObject(DictionaryWithParentSnapshot snapshot)
		{
			Properties = snapshot;
		}

		internal override bool DeepEquals(RavenJToken other)
		{
			var t = other as RavenJObject;
			if (t == null)
				return false;

			return base.DeepEquals(other);
		}

		/// <summary>
		/// Gets the <see cref="RavenJToken"/> with the specified key converted to the specified type.
		/// </summary>
		/// <typeparam name="T">The type to convert the token to.</typeparam>
		/// <param name="key">The token key.</param>
		/// <returns>The converted token value.</returns>
		public override T Value<T>(string key)
		{
			return this[key].Convert<T>();
		}

		/// <summary>
		/// Gets or sets the <see cref="RavenJToken"/> with the specified property name.
		/// </summary>
		/// <value></value>
		public RavenJToken this[string propertyName]
		{
			get
			{
				RavenJToken ret;
				Properties.TryGetValue(propertyName, out ret);
				return ret;
			}
			set { Properties[propertyName] = value; }
		}

		public override RavenJToken CloneToken()
		{
			return CloneTokenImpl(new RavenJObject());
		}

		internal override IEnumerable<KeyValuePair<string, RavenJToken>> GetCloningEnumerator()
		{
			return Properties;
		}

		/// <summary>
		/// Creates a <see cref="RavenJObject"/> from an object.
		/// </summary>
		/// <param name="o">The object that will be used to create <see cref="RavenJObject"/>.</param>
		/// <returns>A <see cref="RavenJObject"/> with the values of the specified object</returns>
		public static new RavenJObject FromObject(object o)
		{
			return FromObject(o, JsonExtensions.CreateDefaultJsonSerializer());
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
			if (reader.TokenType == JsonToken.None)
			{
				if (!reader.Read())
					throw new Exception("Error reading RavenJObject from JsonReader.");
			}

			if (reader.TokenType != JsonToken.StartObject)
				throw new Exception(
					"Error reading RavenJObject from JsonReader. Current JsonReader item is not an object: {0}".FormatWith(CultureInfo.InvariantCulture, reader.TokenType));

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
							o[propName] = val; // TODO: Assert when o.Properties.ContainsKey and its value != val
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
							o[propName] = val; // TODO: Assert when o.Properties.ContainsKey and its value != val
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
							o[propName] = val; // TODO: Assert when o.Properties.ContainsKey and its value != val
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

			throw new Exception("Error reading RavenJObject from JsonReader.");
		}

		/// <summary>
		/// Load a <see cref="RavenJObject"/> from a string that contains JSON.
		/// </summary>
		/// <param name="json">A <see cref="String"/> that contains JSON.</param>
		/// <returns>A <see cref="RavenJObject"/> populated from the string that contains JSON.</returns>
		public new static RavenJObject Parse(string json)
		{
			try
			{
				JsonReader jsonReader = new RavenJsonTextReader(new StringReader(json));
				return Load(jsonReader);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not parse json:" + Environment.NewLine + json, e);
			}
		}

		/// <summary>
		/// Writes this token to a <see cref="JsonWriter"/>.
		/// </summary>
		/// <param name="writer">A <see cref="JsonWriter"/> into which this method will write.</param>
		/// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
		public override void WriteTo(JsonWriter writer, params JsonConverter[] converters)
		{
			writer.WriteStartObject();

			if (Properties != null)
			{
				foreach (var property in Properties)
				{
					writer.WritePropertyName(property.Key);
					if(property.Value == null)
						writer.WriteNull();
					else
						property.Value.WriteTo(writer, converters);
				}
			}

			writer.WriteEndObject();
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

		internal override void AddForCloning(string key, RavenJToken token)
		{
			Properties[key] = token;
		}

		public bool Remove(string propName)
		{
			return Properties.Remove(propName);
		}

		public bool ContainsKey(string key)
		{
			return Properties.ContainsKey(key);
		}

		public bool TryGetValue(string name, out RavenJToken value)
		{
			return Properties.TryGetValue(name, out value);	
		}

		public override RavenJToken CreateSnapshot()
		{
			return new RavenJObject(Properties.CreateSnapshot());
		}

		public override void EnsureSnapshot()
		{
			Properties.EnsureSnapshot();
		}

		public override IEnumerable<RavenJToken> Values()
		{
			return Properties.Values;
		}

		public override IEnumerable<T> Values<T>()
		{
			return Properties.Values.Convert<T>();
		}
	}
}
