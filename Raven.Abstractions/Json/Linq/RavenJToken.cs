using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
	/// <summary>
	/// Represents an abstract JSON token.
	/// </summary>
	public abstract class RavenJToken
	{
		/// <summary>
		/// Gets the node type for this <see cref="RavenJToken"/>.
		/// </summary>
		/// <value>The type.</value>
		public abstract JTokenType Type { get; }

		/// <summary>
		/// Clones this object
		/// </summary>
		/// <returns>A cloned RavenJToken</returns>
		public abstract RavenJToken CloneToken();

		public abstract void EnsureSnapshot();

		public abstract RavenJToken CreateSnapshot();

		protected RavenJToken CloneTokenImpl(RavenJToken newObject)
		{
			var readingStack = new Stack<IEnumerable<KeyValuePair<string, RavenJToken>>>();
			var writingStack = new Stack<RavenJToken>();

			writingStack.Push(newObject);
			readingStack.Push(GetCloningEnumerator());

			while (readingStack.Count > 0)
			{
				var curReader = readingStack.Pop();
				var curObject = writingStack.Pop();
				foreach (var current in curReader)
				{
					if (current.Value == null)
					{
						curObject.AddForCloning(current.Key, null); // we call this explicitly to support null entries in JArray
						continue;
					}
					if (current.Value is RavenJValue)
					{
						curObject.AddForCloning(current.Key, current.Value.CloneToken());
						continue;
					}

					var newVal = current.Value is RavenJArray ? (RavenJToken)new RavenJArray() : new RavenJObject();

					curObject.AddForCloning(current.Key, newVal);

					writingStack.Push(newVal);
					readingStack.Push(current.Value.GetCloningEnumerator());
				}
			}
			return newObject;
		}

		internal static RavenJToken FromObjectInternal(object o, JsonSerializer jsonSerializer)
		{
			var ravenJToken = o as RavenJToken;
			if (ravenJToken != null)
				return ravenJToken;

			RavenJToken token;
			using (var jsonWriter = new RavenJTokenWriter())
			{
				jsonSerializer.Serialize(jsonWriter, o);
				token = jsonWriter.Token;
			}

			return token;
		}

		/// <summary>
		/// Creates a <see cref="RavenJToken"/> from an object.
		/// </summary>
		/// <param name="o">The object that will be used to create <see cref="RavenJToken"/>.</param>
		/// <returns>A <see cref="RavenJToken"/> with the value of the specified object</returns>
		public static RavenJToken FromObject(object o)
		{
			return FromObjectInternal(o, JsonExtensions.CreateDefaultJsonSerializer());
		}

		/// <summary>
		/// Creates a <see cref="RavenJToken"/> from an object using the specified <see cref="JsonSerializer"/>.
		/// </summary>
		/// <param name="o">The object that will be used to create <see cref="RavenJToken"/>.</param>
		/// <param name="jsonSerializer">The <see cref="JsonSerializer"/> that will be used when reading the object.</param>
		/// <returns>A <see cref="RavenJToken"/> with the value of the specified object</returns>
		public static RavenJToken FromObject(object o, JsonSerializer jsonSerializer)
		{
			return FromObjectInternal(o, jsonSerializer);
		}

		/// <summary>
		/// Returns the indented JSON for this token.
		/// </summary>
		/// <returns>
		/// The indented JSON for this token.
		/// </returns>
		public override string ToString()
		{
			return ToString(Formatting.Indented);
		}

		/// <summary>
		/// Returns the JSON for this token using the given formatting and converters.
		/// </summary>
		/// <param name="formatting">Indicates how the output is formatted.</param>
		/// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
		/// <returns>The JSON for this token using the given formatting and converters.</returns>
		public string ToString(Formatting formatting, params JsonConverter[] converters)
		{
			using (var sw = new StringWriter(CultureInfo.InvariantCulture))
			{
				var jw = new JsonTextWriter(sw);
				jw.Formatting = formatting;

				WriteTo(jw, converters);

				return sw.ToString();
			}
		}

		/// <summary>
		/// Writes this token to a <see cref="JsonWriter"/>.
		/// </summary>
		/// <param name="writer">A <see cref="JsonWriter"/> into which this method will write.</param>
		/// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
		public abstract void WriteTo(JsonWriter writer, params JsonConverter[] converters);

		/// <summary>
		/// Creates a <see cref="RavenJToken"/> from a <see cref="JsonReader"/>.
		/// </summary>
		/// <param name="reader">An <see cref="JsonReader"/> positioned at the token to read into this <see cref="RavenJToken"/>.</param>
		/// <returns>
		/// An <see cref="RavenJToken"/> that contains the token and its descendant tokens
		/// that were read from the reader. The runtime type of the token is determined
		/// by the token type of the first token encountered in the reader.
		/// </returns>
		public static RavenJToken ReadFrom(JsonReader reader)
		{
			if (reader.TokenType == JsonToken.None)
			{
				if (!reader.Read())
					throw new Exception("Error reading RavenJToken from JsonReader.");
			}

			switch (reader.TokenType)
			{
				case JsonToken.StartObject:
					return RavenJObject.Load(reader);
				case JsonToken.StartArray:
					return RavenJArray.Load(reader);
				case JsonToken.String:
				case JsonToken.Integer:
				case JsonToken.Float:
				case JsonToken.Date:
				case JsonToken.Boolean:
				case JsonToken.Bytes:
				case JsonToken.Null:
				case JsonToken.Undefined:
					return new RavenJValue(reader.Value);
			}

			throw new Exception("Error reading RavenJToken from JsonReader. Unexpected token: {0}".FormatWith(CultureInfo.InvariantCulture, reader.TokenType));
		}

		/// <summary>
		/// Load a <see cref="RavenJToken"/> from a string that contains JSON.
		/// </summary>
		/// <param name="json">A <see cref="String"/> that contains JSON.</param>
		/// <returns>A <see cref="RavenJToken"/> populated from the string that contains JSON.</returns>
		public static RavenJToken Parse(string json)
		{
			try
			{
				JsonReader jsonReader = new RavenJsonTextReader(new StringReader(json));

				return Load(jsonReader);
			}
			catch (Exception e)
			{
				throw new JsonSerializationException("Could not parse: [" + json + "]", e);
			}
		}

		public static RavenJToken TryLoad(Stream stream)
		{
			var jsonTextReader = new RavenJsonTextReader(new StreamReader(stream));
			if (jsonTextReader.Read() == false || jsonTextReader.TokenType == JsonToken.None)
			{
				return null;
			}
			return Load(jsonTextReader);
		}

		/// <summary>
		/// Creates a <see cref="RavenJToken"/> from a <see cref="JsonReader"/>.
		/// </summary>
		/// <param name="reader">An <see cref="JsonReader"/> positioned at the token to read into this <see cref="RavenJToken"/>.</param>
		/// <returns>
		/// An <see cref="RavenJToken"/> that contains the token and its descendant tokens
		/// that were read from the reader. The runtime type of the token is determined
		/// by the token type of the first token encountered in the reader.
		/// </returns>
		public static RavenJToken Load(JsonReader reader)
		{
			return ReadFrom(reader);
		}

		/// <summary>
		/// Gets the <see cref="RavenJToken"/> with the specified key converted to the specified type.
		/// </summary>
		/// <typeparam name="T">The type to convert the token to.</typeparam>
		/// <param name="key">The token key.</param>
		/// <returns>The converted token value.</returns>
		public virtual T Value<T>(string key)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		/// Compares the values of two tokens, including the values of all descendant tokens.
		/// </summary>
		/// <param name="t1">The first <see cref="RavenJToken"/> to compare.</param>
		/// <param name="t2">The second <see cref="RavenJToken"/> to compare.</param>
		/// <returns>true if the tokens are equal; otherwise false.</returns>
		public static bool DeepEquals(RavenJToken t1, RavenJToken t2)
		{
			return (t1 == t2 || (t1 != null && t2 != null && t1.DeepEquals(t2)));
		}

		public static int GetDeepHashCode(RavenJToken t)
		{
			return t == null ? 0 : t.GetDeepHashCode();
		}

		internal virtual bool DeepEquals(RavenJToken other)
		{
			if (other == null)
				return false;

			if (Type != other.Type)
				return false;

			var otherStack = new Stack<RavenJToken>();
			var thisStack = new Stack<RavenJToken>();

			thisStack.Push(this);
			otherStack.Push(other);

			while (otherStack.Count > 0)
			{
				var curOtherReader = otherStack.Pop();
				var curThisReader = thisStack.Pop();

				if(curOtherReader == null && curThisReader == null)
					continue; // shouldn't happen, but we got an error report from a user about this
				if (curOtherReader == null || curThisReader == null)
					return false;

				if (curThisReader.Type == curOtherReader.Type)
				{
					switch (curOtherReader.Type)
					{
						case JTokenType.Array:
							var selfArray = (RavenJArray)curThisReader;
							var otherArray = (RavenJArray)curOtherReader;
							if (selfArray.Length != otherArray.Length)
								return false;

							for (int i = 0; i < selfArray.Length; i++)
							{
								thisStack.Push(selfArray[i]);
								otherStack.Push(otherArray[i]);
							}
							break;
						case JTokenType.Object:
							var selfObj = (RavenJObject)curThisReader;
							var otherObj = (RavenJObject)curOtherReader;
							if (selfObj.Count != otherObj.Count)
								return false;

							foreach (var kvp in selfObj.Properties)
							{
								RavenJToken token;
								if (otherObj.TryGetValue(kvp.Key, out token) == false)
									return false;
								switch (kvp.Value.Type)
								{
									case JTokenType.Array:
									case JTokenType.Object:
										otherStack.Push(token);
										thisStack.Push(kvp.Value);
										break;
									case JTokenType.Bytes:
										var bytes = kvp.Value.Value<byte[]>();
										byte[] tokenBytes = token.Type == JTokenType.String
																? Convert.FromBase64String(token.Value<string>())
																: token.Value<byte[]>();
										if (bytes.Length != tokenBytes.Length)
											return false;

										if (tokenBytes.Where((t, i) => t != bytes[i]).Any())
										{
											return false;
										}

										break;
									default:
										if (!kvp.Value.DeepEquals(token))
											return false;
										break;
								}
							}
							break;
						default:
							if (!curOtherReader.DeepEquals(curThisReader))
								return false;
							break;
					}
				}
				else
				{
					switch (curThisReader.Type)
					{
						case JTokenType.Guid:
							if (curOtherReader.Type != JTokenType.String)
								return false;

							if (curThisReader.Value<string>() != curOtherReader.Value<string>())
								return false;

							break;
						default:
							return false;
					}
				}
			}

			return true;
		}

		internal virtual int GetDeepHashCode()
		{
			var stack = new Stack<Tuple<int, RavenJToken>>();
			int ret = 0;

			stack.Push(Tuple.Create(0, this));
			while (stack.Count > 0)
			{
				var cur = stack.Pop();

				if (cur.Item2.Type == JTokenType.Array)
				{
					var arr = (RavenJArray)cur.Item2;
					for (int i = 0; i < arr.Length; i++)
					{
						stack.Push(Tuple.Create(cur.Item1 ^ (i * 397), arr[i]));
					}
				}
				else if (cur.Item2.Type == JTokenType.Object)
				{
					var selfObj = (RavenJObject)cur.Item2;
					foreach (var kvp in selfObj.Properties)
					{
						stack.Push(Tuple.Create(cur.Item1 ^ (397 * kvp.Key.GetHashCode()), kvp.Value));
					}
				}
				else // value
				{
					ret ^= cur.Item1 ^ (cur.Item2.GetDeepHashCode() * 397);
				}
			}

			return ret;
		}


		/// <summary>
		/// Selects the token that matches the object path.
		/// </summary>
		/// <param name="path">
		/// The object path from the current <see cref="RavenJToken"/> to the <see cref="RavenJToken"/>
		/// to be returned. This must be a string of property names or array indexes separated
		/// by periods, such as <code>Tables[0].DefaultView[0].Price</code> in C# or
		/// <code>Tables(0).DefaultView(0).Price</code> in Visual Basic.
		/// </param>
		/// <returns>The <see cref="RavenJToken"/> that matches the object path or a null reference if no matching token is found.</returns>
		public RavenJToken SelectToken(string path)
		{
			return SelectToken(path, false);
		}

		/// <summary>
		/// Selects the token that matches the object path.
		/// </summary>
		/// <param name="path">
		/// The object path from the current <see cref="RavenJToken"/> to the <see cref="RavenJToken"/>
		/// to be returned. This must be a string of property names or array indexes separated
		/// by periods, such as <code>Tables[0].DefaultView[0].Price</code> in C# or
		/// <code>Tables(0).DefaultView(0).Price</code> in Visual Basic.
		/// </param>
		/// <param name="errorWhenNoMatch">A flag to indicate whether an error should be thrown if no token is found.</param>
		/// <returns>The <see cref="RavenJToken"/> that matches the object path.</returns>
		public RavenJToken SelectToken(string path, bool errorWhenNoMatch)
		{
			var p = new RavenJPath(path);
			return p.Evaluate(this, errorWhenNoMatch);
		}

		/// <summary>
		/// Returns a collection of the child values of this token, in document order.
		/// </summary>
		/// <typeparam name="T">The type to convert the values to.</typeparam>
		/// <returns>
		/// A <see cref="IEnumerable{T}"/> containing the child values of this <see cref="RavenJToken"/>, in document order.
		/// </returns>
		public virtual IEnumerable<T> Values<T>()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		/// Returns a collection of the child values of this token, in document order.
		/// </summary>
		public virtual IEnumerable<RavenJToken> Values()
		{
			throw new NotSupportedException();
		}
		internal virtual void AddForCloning(string key, RavenJToken token)
		{
			// kept virtual (as opposed to abstract) to waive the new for implementing this in RavenJValue
		}

		internal virtual IEnumerable<KeyValuePair<string, RavenJToken>> GetCloningEnumerator()
		{
			return null;
		}

		#region Cast to operators
		/// <summary>
		/// Performs an implicit conversion from <see cref="Boolean"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(bool value)
		{
			return new RavenJValue(value);
		}

#if !PocketPC && !NET20
		/// <summary>
		/// Performs an implicit conversion from <see cref="DateTimeOffset"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(DateTimeOffset value)
		{
			return new RavenJValue(value);
		}
#endif

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Boolean}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(bool? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Int64}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(long value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{DateTime}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(DateTime? value)
		{
			return new RavenJValue(value);
		}

#if !PocketPC && !NET20
		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{DateTimeOffset}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(DateTimeOffset? value)
		{
			return new RavenJValue(value);
		}
#endif

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Decimal}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(decimal? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Double}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(double? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Int16"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(short value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="UInt16"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(ushort value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Int32"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(int value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Int32}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(int? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="DateTime"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(DateTime value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Int64}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(long? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Single}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(float? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Decimal"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(decimal value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{Int16}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(short? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{UInt16}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(ushort? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{UInt32}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(uint? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Nullable{UInt64}"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(ulong? value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Double"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(double value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="Single"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(float value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="String"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(string value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="UInt32"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(uint value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="UInt64"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public static implicit operator RavenJToken(ulong value)
		{
			return new RavenJValue(value);
		}

		/// <summary>
		/// Performs an implicit conversion from <see cref="T:System.Byte[]"/> to <see cref="RavenJToken"/>.
		/// </summary>
		/// <param name="value">The value to create a <see cref="RavenJValue"/> from.</param>
		/// <returns>The <see cref="RavenJValue"/> initialized with the specified value.</returns>
		public static implicit operator RavenJToken(byte[] value)
		{
			return new RavenJValue(value);
		}
		#endregion
	}
}
