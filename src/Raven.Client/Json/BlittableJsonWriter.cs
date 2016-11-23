//using System;
//using System.Collections.Generic;
//using System.IO;
//using Raven.Imports.Newtonsoft.Json;
//using Raven.Imports.Newtonsoft.Json.Utilities;
//using Sparrow.Json;
//using Sparrow.Json.Parsing;

//namespace Raven.Client.Json
//{
//    public partial class BlittableJsonWriter:JsonWriter
//    {
//        private BlittableJsonDocumentBuilder _builder;
//        private JsonParserState _jsonParserState;
//        private BlittableJsonWriterParser _jsonParser;

//        public BlittableJsonWriter(JsonOperationContext context)
//        {
//            _jsonParserState = new JsonParserState();
//            _builder = new BlittableJsonDocumentBuilder(context, _jsonParserState,this);
            
//        }

//        public BlittableJsonReaderObject GetReader()
//        {
//            return _builder.CreateReader();
//        }

//        public override void Flush()
//        {
//            _builder.Read();
//        }

//        internal virtual void OnStringEscapeHandlingChanged()
//        {
//            // hacky but there is a calculated value that relies on StringEscapeHandling
//        }


//        private JsonContainerType Peek()
//        {
//            return _currentPosition.Type;
//        }
        

//        /// <summary>
//        /// Closes this stream and the underlying stream.
//        /// </summary>
//        public virtual void Close()
//        {
//            base.Close();
//            // todo: finalize document, create reader?
//            _builder.FinalizeDocument();
            
//        }

//        /// <summary>
//        /// Writes the beginning of a JSON object.
//        /// </summary>
//        public virtual void WriteStartObject()
//        {
//            InternalWriteStart(JsonToken.StartObject, JsonContainerType.Object);
//        }

//        /// <summary>
//        /// Writes the end of a JSON object.
//        /// </summary>
//        public virtual void WriteEndObject()
//        {
//            InternalWriteEnd(JsonContainerType.Object);
//        }

//        /// <summary>
//        /// Writes the beginning of a JSON array.
//        /// </summary>
//        public virtual void WriteStartArray()
//        {
//            InternalWriteStart(JsonToken.StartArray, JsonContainerType.Array);
//        }

//        /// <summary>
//        /// Writes the end of an array.
//        /// </summary>
//        public virtual void WriteEndArray()
//        {
//            InternalWriteEnd(JsonContainerType.Array);
//        }

//        /// <summary>
//        /// Writes the start of a constructor with the given name.
//        /// </summary>
//        /// <param name="name">The name of the constructor.</param>
//        public virtual void WriteStartConstructor(string name)
//        {
//            InternalWriteStart(JsonToken.StartConstructor, JsonContainerType.Constructor);
//        }

//        /// <summary>
//        /// Writes the end constructor.
//        /// </summary>
//        public virtual void WriteEndConstructor()
//        {
//            InternalWriteEnd(JsonContainerType.Constructor);
//        }

//        /// <summary>
//        /// Writes the property name of a name/value pair on a JSON object.
//        /// </summary>
//        /// <param name="name">The name of the property.</param>
//        public virtual void WritePropertyName(string name)
//        {
//            InternalWritePropertyName(name);
//        }

//        /// <summary>
//        /// Writes the property name of a name/value pair on a JSON object.
//        /// </summary>
//        /// <param name="name">The name of the property.</param>
//        /// <param name="escape">A flag to indicate whether the text should be escaped when it is written as a JSON property name.</param>
//        public virtual void WritePropertyName(string name, bool escape)
//        {
//            WritePropertyName(name);
//        }

//        /// <summary>
//        /// Writes the end of the current JSON object or array.
//        /// </summary>
//        public virtual void WriteEnd()
//        {
//            WriteEnd(Peek());
//        }

//        internal virtual void WriteToken(JsonReader reader, bool writeChildren, bool writeDateConstructorAsDate, bool writeComments)
//        {
//            int initialDepth;

//            if (reader.TokenType == JsonToken.None)
//            {
//                initialDepth = -1;
//            }
//            else if (!JsonTokenUtils.IsStartToken(reader.TokenType))
//            {
//                initialDepth = reader.Depth + 1;
//            }
//            else
//            {
//                initialDepth = reader.Depth;
//            }

//            do
//            {
//                // write a JValue date when the constructor is for a date
//                if (writeDateConstructorAsDate && reader.TokenType == JsonToken.StartConstructor && string.Equals(reader.Value.ToString(), "Date", StringComparison.Ordinal))
//                {
//                    WriteConstructorDate(reader);
//                }
//                else
//                {
//                    if (writeComments || reader.TokenType != JsonToken.Comment)
//                    {
//                        WriteToken(reader.TokenType, reader.Value);
//                    }
//                }
//            } while (
//                // stop if we have reached the end of the token being read
//                initialDepth - 1 < reader.Depth - (JsonTokenUtils.IsEndToken(reader.TokenType) ? 1 : 0)
//                && writeChildren
//                && reader.Read());
//        }

//        /// <summary>
//        /// Writes the specified end token.
//        /// </summary>
//        /// <param name="token">The end token to write.</param>
//        protected virtual void WriteEnd(JsonToken token)
//        {
//        }

//        /// <summary>
//        /// Writes indent characters.
//        /// </summary>
//        protected virtual void WriteIndent()
//        {
//        }

//        /// <summary>
//        /// Writes the JSON value delimiter.
//        /// </summary>
//        protected virtual void WriteValueDelimiter()
//        {
//        }

//        /// <summary>
//        /// Writes an indent space.
//        /// </summary>
//        protected virtual void WriteIndentSpace()
//        {
//        }


//        #region WriteValue methods
//        /// <summary>
//        /// Writes a null value.
//        /// </summary>
//        public virtual void WriteNull()
//        {
//            InternalWriteValue(JsonToken.Null);
//        }

//        /// <summary>
//        /// Writes an undefined value.
//        /// </summary>
//        public virtual void WriteUndefined()
//        {
//            InternalWriteValue(JsonToken.Undefined);
//        }

//        /// <summary>
//        /// Writes raw JSON without changing the writer's state.
//        /// </summary>
//        /// <param name="json">The raw JSON to write.</param>
//        public virtual void WriteRaw(string json)
//        {
//            InternalWriteRaw();
//        }

//        /// <summary>
//        /// Writes raw JSON where a value is expected and updates the writer's state.
//        /// </summary>
//        /// <param name="json">The raw JSON to write.</param>
//        public virtual void WriteRawValue(string json)
//        {
//            // hack. want writer to change state as if a value had been written
//            UpdateScopeWithFinishedValue();
//            AutoComplete(JsonToken.Undefined);
//            WriteRaw(json);
//        }

//        /// <summary>
//        /// Writes a <see cref="String"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="String"/> value to write.</param>
//        public virtual void WriteValue(string value)
//        {
//            InternalWriteValue(JsonToken.String);
//        }

//        /// <summary>
//        /// Writes a <see cref="Int32"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Int32"/> value to write.</param>
//        public virtual void WriteValue(int value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="UInt32"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="UInt32"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(uint value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="Int64"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Int64"/> value to write.</param>
//        public virtual void WriteValue(long value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="UInt64"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="UInt64"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(ulong value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="Single"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Single"/> value to write.</param>
//        public virtual void WriteValue(float value)
//        {
//            InternalWriteValue(JsonToken.Float);
//        }

//        /// <summary>
//        /// Writes a <see cref="Double"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Double"/> value to write.</param>
//        public virtual void WriteValue(double value)
//        {
//            InternalWriteValue(JsonToken.Float);
//        }

//        /// <summary>
//        /// Writes a <see cref="Boolean"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Boolean"/> value to write.</param>
//        public virtual void WriteValue(bool value)
//        {
//            InternalWriteValue(JsonToken.Boolean);
//        }

//        /// <summary>
//        /// Writes a <see cref="Int16"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Int16"/> value to write.</param>
//        public virtual void WriteValue(short value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="UInt16"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="UInt16"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(ushort value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="Char"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Char"/> value to write.</param>
//        public virtual void WriteValue(char value)
//        {
//            InternalWriteValue(JsonToken.String);
//        }

//        /// <summary>
//        /// Writes a <see cref="Byte"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Byte"/> value to write.</param>
//        public virtual void WriteValue(byte value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="SByte"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="SByte"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(sbyte value)
//        {
//            InternalWriteValue(JsonToken.Integer);
//        }

//        /// <summary>
//        /// Writes a <see cref="Decimal"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Decimal"/> value to write.</param>
//        public virtual void WriteValue(decimal value)
//        {
//            InternalWriteValue(JsonToken.Float);
//        }

//        /// <summary>
//        /// Writes a <see cref="DateTime"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="DateTime"/> value to write.</param>
//        public virtual void WriteValue(DateTime value)
//        {
//            InternalWriteValue(JsonToken.Date);
//        }

//#if !NET20
//        /// <summary>
//        /// Writes a <see cref="DateTimeOffset"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="DateTimeOffset"/> value to write.</param>
//        public virtual void WriteValue(DateTimeOffset value)
//        {
//            InternalWriteValue(JsonToken.Date);
//        }
//#endif

//        /// <summary>
//        /// Writes a <see cref="Guid"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Guid"/> value to write.</param>
//        public virtual void WriteValue(Guid value)
//        {
//            InternalWriteValue(JsonToken.String);
//        }

//        /// <summary>
//        /// Writes a <see cref="TimeSpan"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="TimeSpan"/> value to write.</param>
//        public virtual void WriteValue(TimeSpan value)
//        {
//            InternalWriteValue(JsonToken.String);
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Int32}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Int32}"/> value to write.</param>
//        public virtual void WriteValue(int? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{UInt32}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{UInt32}"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(uint? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Int64}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Int64}"/> value to write.</param>
//        public virtual void WriteValue(long? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{UInt64}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{UInt64}"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(ulong? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Single}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Single}"/> value to write.</param>
//        public virtual void WriteValue(float? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Double}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Double}"/> value to write.</param>
//        public virtual void WriteValue(double? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Boolean}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Boolean}"/> value to write.</param>
//        public virtual void WriteValue(bool? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Int16}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Int16}"/> value to write.</param>
//        public virtual void WriteValue(short? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{UInt16}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{UInt16}"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(ushort? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Char}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Char}"/> value to write.</param>
//        public virtual void WriteValue(char? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Byte}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Byte}"/> value to write.</param>
//        public virtual void WriteValue(byte? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{SByte}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{SByte}"/> value to write.</param>
//        [CLSCompliant(false)]
//        public virtual void WriteValue(sbyte? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{Decimal}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Decimal}"/> value to write.</param>
//        public virtual void WriteValue(decimal? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{DateTime}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{DateTime}"/> value to write.</param>
//        public virtual void WriteValue(DateTime? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//#if !NET20
//        /// <summary>
//        /// Writes a <see cref="Nullable{DateTimeOffset}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{DateTimeOffset}"/> value to write.</param>
//        public virtual void WriteValue(DateTimeOffset? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }
//#endif

//        /// <summary>
//        /// Writes a <see cref="Nullable{Guid}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{Guid}"/> value to write.</param>
//        public virtual void WriteValue(Guid? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Nullable{TimeSpan}"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Nullable{TimeSpan}"/> value to write.</param>
//        public virtual void WriteValue(TimeSpan? value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                WriteValue(value.GetValueOrDefault());
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Byte"/>[] value.
//        /// </summary>
//        /// <param name="value">The <see cref="Byte"/>[] value to write.</param>
//        public virtual void WriteValue(byte[] value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                InternalWriteValue(JsonToken.Bytes);
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Uri"/> value.
//        /// </summary>
//        /// <param name="value">The <see cref="Uri"/> value to write.</param>
//        public virtual void WriteValue(Uri value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//                InternalWriteValue(JsonToken.String);
//            }
//        }

//        /// <summary>
//        /// Writes a <see cref="Object"/> value.
//        /// An error will raised if the value cannot be written as a single JSON token.
//        /// </summary>
//        /// <param name="value">The <see cref="Object"/> value to write.</param>
//        public virtual void WriteValue(object value)
//        {
//            if (value == null)
//            {
//                WriteNull();
//            }
//            else
//            {
//#if !(NET20 || NET35 || PORTABLE || PORTABLE40) || NETSTANDARD1_1
//                // this is here because adding a WriteValue(BigInteger) to JsonWriter will
//                // mean the user has to add a reference to System.Numerics.dll
//                if (value is BigInteger)
//                {
//                    throw CreateUnsupportedTypeException(this, value);
//                }
//#endif

//                WriteValue(this, ConvertUtils.GetTypeCode(value.GetType()), value);
//            }
//        }
//        #endregion

//        /// <summary>
//        /// Writes out a comment <code>/*...*/</code> containing the specified text. 
//        /// </summary>
//        /// <param name="text">Text to place inside the comment.</param>
//        public virtual void WriteComment(string text)
//        {
//            InternalWriteComment();
//        }

//        /// <summary>
//        /// Writes out the given white space.
//        /// </summary>
//        /// <param name="ws">The string of white space characters.</param>
//        public virtual void WriteWhitespace(string ws)
//        {
//            InternalWriteWhitespace(ws);
//        }

//        /// <summary>
//        /// Releases unmanaged and - optionally - managed resources
//        /// </summary>
//        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
//        protected virtual void Dispose(bool disposing)
//        {
//            if (_currentState != State.Closed && disposing)
//            {
//                Close();
//            }
//        }


//    }
//}