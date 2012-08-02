using System;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Json.Linq
{
	/// <summary>
	/// Represents a writer that provides a fast, non-cached, forward-only way of generating Json data.
	/// </summary>
	public class RavenJTokenWriter : JsonWriter
	{
		private RavenJToken _token;
		private RavenJValue _value;
		private readonly Stack<RavenJToken> _tokenStack = new Stack<RavenJToken>();

		protected RavenJToken CurrentToken { get { return (_tokenStack.Count == 0) ? null : _tokenStack.Peek(); } }

		/// <summary>
		/// Gets the token being writen.
		/// </summary>
		/// <value>The token being writen.</value>
		public RavenJToken Token
		{
			get
			{
				if (_token != null)
					return _token;

				return _value;
			}
		}

		/// <summary>
		/// Flushes whatever is in the buffer to the underlying streams and also flushes the underlying stream.
		/// </summary>
		public override void Flush()
		{
		}

		private string _tempPropName;

		public override void WritePropertyName(string name)
		{
			base.WritePropertyName(name);

			if (_tempPropName != null)
				throw new JsonWriterException("Was not expecting a propery name here");

			_tempPropName = name;
		}

		private void AddParent(RavenJToken token)
		{
			if (_token == null)
			{
				_token = token;
				_tokenStack.Push(_token);
				return;
			}

			switch (CurrentToken.Type)
			{
				case JTokenType.Object:
					((RavenJObject)CurrentToken)[_tempPropName] = token;
					_tempPropName = null;
					break;
				case JTokenType.Array:
					((RavenJArray)CurrentToken).Add(token);
					break;
				default:
					throw new JsonWriterException("Unexpected token: " + CurrentToken.Type);
			}

			_tokenStack.Push(token);
		}

		private void RemoveParent()
		{
			_tokenStack.Pop();
		}

		public override void WriteStartObject()
		{
			base.WriteStartObject();

			AddParent(new RavenJObject());
		}

		/// <summary>
		/// Writes the beginning of a Json array.
		/// </summary>
		public override void WriteStartArray()
		{
			base.WriteStartArray();

			AddParent(new RavenJArray());
		}

		/// <summary>
		/// Writes the end.
		/// </summary>
		/// <param name="token">The token.</param>
		protected override void WriteEnd(JsonToken token)
		{
			RemoveParent();
		}

		private void AddValue(object value, JsonToken token)
		{
			AddValue(new RavenJValue(value), token);
		}

		internal void AddValue(RavenJValue value, JsonToken token)
		{
			if (_tokenStack.Count == 0)
				_value = value;
			else
			{
				switch (CurrentToken.Type)
				{
					case JTokenType.Object:
						((RavenJObject)CurrentToken)[_tempPropName] = value;
						_tempPropName = null;
						break;
					case JTokenType.Array:
						((RavenJArray)CurrentToken).Add(value);
						break;
					default:
						throw new JsonWriterException("Unexpected token: " + token);
				}
			}
		}

		public override void WriteRaw(string json)
		{
			throw new NotSupportedException();
		}

		#region WriteValue methods
		/// <summary>
		/// Writes a null value.
		/// </summary>
		public override void WriteNull()
		{
			base.WriteNull();
			AddValue(new RavenJValue(null, JTokenType.Null), JsonToken.Null);
		}

		/// <summary>
		/// Writes an undefined value.
		/// </summary>
		public override void WriteUndefined()
		{
			base.WriteUndefined();
			AddValue(new RavenJValue(null, JTokenType.Null), JsonToken.Undefined);
		}

		/// <summary>
		/// Writes a <see cref="String"/> value.
		/// </summary>
		/// <param name="value">The <see cref="String"/> value to write.</param>
		public override void WriteValue(string value)
		{
			base.WriteValue(value);
			if (value == null)
			{
				AddValue(new RavenJValue(null, JTokenType.Null), JsonToken.Null);
			}
			else
			{
				AddValue(value, JsonToken.String);
			}
		}

		/// <summary>
		/// Writes a <see cref="Int32"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Int32"/> value to write.</param>
		public override void WriteValue(int value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="UInt32"/> value.
		/// </summary>
		/// <param name="value">The <see cref="UInt32"/> value to write.</param>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public override void WriteValue(uint value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="Int64"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Int64"/> value to write.</param>
		public override void WriteValue(long value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="UInt64"/> value.
		/// </summary>
		/// <param name="value">The <see cref="UInt64"/> value to write.</param>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public override void WriteValue(ulong value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="Single"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Single"/> value to write.</param>
		public override void WriteValue(float value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Float);
		}

		/// <summary>
		/// Writes a <see cref="Double"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Double"/> value to write.</param>
		public override void WriteValue(double value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Float);
		}

		/// <summary>
		/// Writes a <see cref="Boolean"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Boolean"/> value to write.</param>
		public override void WriteValue(bool value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Boolean);
		}

		/// <summary>
		/// Writes a <see cref="Int16"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Int16"/> value to write.</param>
		public override void WriteValue(short value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="UInt16"/> value.
		/// </summary>
		/// <param name="value">The <see cref="UInt16"/> value to write.</param>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public override void WriteValue(ushort value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="Char"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Char"/> value to write.</param>
		public override void WriteValue(char value)
		{
			base.WriteValue(value);
			AddValue(value.ToString(), JsonToken.String);
		}

		/// <summary>
		/// Writes a <see cref="Byte"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Byte"/> value to write.</param>
		public override void WriteValue(byte value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="SByte"/> value.
		/// </summary>
		/// <param name="value">The <see cref="SByte"/> value to write.</param>
#if !SILVERLIGHT
		[CLSCompliant(false)]
#endif
		public override void WriteValue(sbyte value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Integer);
		}

		/// <summary>
		/// Writes a <see cref="Decimal"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Decimal"/> value to write.</param>
		public override void WriteValue(decimal value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Float);
		}

		/// <summary>
		/// Writes a <see cref="DateTime"/> value.
		/// </summary>
		/// <param name="value">The <see cref="DateTime"/> value to write.</param>
		public override void WriteValue(DateTime value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Date);
		}

#if !PocketPC && !NET20
		/// <summary>
		/// Writes a <see cref="DateTimeOffset"/> value.
		/// </summary>
		/// <param name="value">The <see cref="DateTimeOffset"/> value to write.</param>
		public override void WriteValue(DateTimeOffset value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Date);
		}
#endif

		/// <summary>
		/// Writes a <see cref="T:Byte[]"/> value.
		/// </summary>
		/// <param name="value">The <see cref="T:Byte[]"/> value to write.</param>
		public override void WriteValue(byte[] value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.Bytes);
		}

		/// <summary>
		/// Writes a <see cref="Guid"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Guid"/> value to write.</param>
		public override void WriteValue(Guid value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.String);
		}

		/// <summary>
		/// Writes a <see cref="TimeSpan"/> value.
		/// </summary>
		/// <param name="value">The <see cref="TimeSpan"/> value to write.</param>
		public override void WriteValue(TimeSpan value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.String);
		}

		/// <summary>
		/// Writes a <see cref="Uri"/> value.
		/// </summary>
		/// <param name="value">The <see cref="Uri"/> value to write.</param>
		public override void WriteValue(Uri value)
		{
			base.WriteValue(value);
			AddValue(value, JsonToken.String);
		}
		
		#endregion
	}
}
