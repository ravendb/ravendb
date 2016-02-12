using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Json.Parsing;
using Sparrow;

namespace Raven.Server.Json
{
    public class BlittableJsonTextWriter : IDisposable
    {
        private readonly Stream _stream;

        private const byte StartObject = (byte)'{';
        private const byte EndObject = (byte)'}';
        private const byte StartArray = (byte)'[';
        private const byte EndArray = (byte)']';
        private const byte Comma = (byte)',';
        private const byte Quote = (byte)'"';
        private const byte Colon = (byte)':';
        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        public static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        private int _pos;
        private readonly byte[] _buffer;
        private Stack<WriteObjectState> _objectState = new Stack<WriteObjectState>();
        private Stack<WriteArrayState> _arrayState = new Stack<WriteArrayState>(); 
        private struct WriteObjectState
        {
            public bool FirstProperty;
            public bool ReadingProperty;
            public bool WroteStartObject;
        }

        private struct WriteArrayState
        {
            public bool FirstItem;
            public bool WriteStartArray;
            public bool ReadingItem;
        }

        public BlittableJsonTextWriter(RavenOperationContext context, Stream stream)
        {
            _stream = stream;
            _buffer = context.GetManagedBuffer();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void WriteString(LazyStringValue str)
        {
            var strBuffer = str.Buffer;
            var size = str.Size;

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
            var escapeSequencePos = size;
            var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                WriteRawString(strBuffer, bytesToSkip);
                strBuffer += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = *(strBuffer++);
                EnsureBuffer(2);
                _buffer[_pos++] = (byte)'\\';
                _buffer[_pos++] = GetEscapeCharacter(b);
            }
            // write remaining (or full string) to the buffer in one shot
            WriteRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        private byte GetEscapeCharacter(byte b)
        {
            switch (b)
            {
                case (byte)'\b':
                    return (byte)'b';
                case (byte)'\t':
                    return (byte)'t';
                case (byte)'\n':
                    return (byte)'n';
                case (byte)'\f':
                    return (byte)'f';
                case (byte)'\r':
                    return (byte)'r';
                case (byte)'\\':
                    return (byte)'\\';
                case (byte)'/':
                    return (byte)'/';
                case (byte)'"':
                    return (byte)'"';
                default:
                    throw new InvalidOperationException("Invalid escape char '" + (char)b + "' numeric value is: " + b);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void WriteString(LazyCompressedStringValue str)
        {
            var strBuffer = str.DecompressToTempBuffer();

            var size = str.UncompressedSize;

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
            var escapeSequencePos = str.CompressedSize;
            var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                WriteRawString(strBuffer, bytesToSkip);
                strBuffer += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = *(strBuffer++);
                EnsureBuffer(2);
                _buffer[_pos++] = (byte)'\\';
                _buffer[_pos++] = GetEscapeCharacter(b);
            }
            // write remaining (or full string) to the buffer in one shot
            WriteRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }


        private unsafe void WriteRawString(byte* buffer, int size)
        {
            if (size < _buffer.Length)
            {
                EnsureBuffer(size);
                fixed (byte* p = _buffer)
                    Memory.Copy(p + _pos, buffer, size);
                _pos += size;
                return;
            }

            // need to do this in pieces
            var posInStr = 0;
            fixed (byte* p = _buffer)
            {
                while (posInStr < size)
                {
                    var amountToCopy = Math.Min(size - posInStr, _buffer.Length);
                    Flush();
                    Memory.Copy(p, buffer + posInStr, amountToCopy);
                    posInStr += amountToCopy;
                    _pos = amountToCopy;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteStartObject()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = StartObject;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEndArray()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = EndArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteStartArray()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = StartArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEndObject()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = EndObject;
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureBuffer(int len)
        {
            if (_pos + len < _buffer.Length)
                return;
            if (len >= _buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(len));

            Flush();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            if (_pos == 0)
                return;
            _stream.Write(_buffer, 0, _pos);
            _pos = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNull()
        {
            EnsureBuffer(4);
            for (int i = 0; i < 4; i++)
            {
                _buffer[_pos++] = NullBuffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBool(bool val)
        {
            EnsureBuffer(5);
            var buffer = val ? TrueBuffer : FalseBuffer;
            for (int i = 0; i < buffer.Length; i++)
            {
                _buffer[_pos++] = buffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteComma()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = Comma;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(LazyStringValue prop)
        {
            WriteString(prop);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        public void WriteInteger(long val)
        {
            if (val == 0)
            {
                EnsureBuffer(1);
                _buffer[_pos++] = (byte)'0';
                return;
            }
            int len = 1;

            for (var i = val / 10; i != 0; i /= 10)
            {
                len++;
            }
            if (val < 0)
            {
                EnsureBuffer(len + 1);
                _buffer[_pos++] = (byte)'-';
            }
            else
            {
                EnsureBuffer(len);
            }
            for (int i = len - 1; i >= 0; i--)
            {
                _buffer[_pos + i] = (byte)('0' + Math.Abs(val % 10));
                val /= 10;
            }
            _pos += len;
        }

        public unsafe void WriteDouble(LazyDoubleValue val)
        {
            var lazyStringValue = val.Inner;
            WriteRawString(lazyStringValue.Buffer, lazyStringValue.Size);
        }

        private unsafe void WriteString(RavenOperationContext context, JsonParserState state)
        {
            WriteString(new LazyStringValue(null, state.StringBuffer, state.StringSize, context));
        }

        public bool WriteObject(RavenOperationContext context, JsonParserState state, IJsonParser parser)
        {
            WriteObjectState writeState;
            if (_objectState.Count != 0)
            {
                writeState = _objectState.Pop();
            }
            else
            {
                writeState = new WriteObjectState
                {
                    FirstProperty = true,
                    ReadingProperty = true,
                    WroteStartObject = false
                };
            }
            if (writeState.FirstProperty && state.CurrentTokenType != JsonParserToken.StartObject)
                throw new InvalidOperationException("StartObject expected, but got " + state.CurrentTokenType);

            if (writeState.WroteStartObject == false)
            {
                WriteStartObject();
                writeState.WroteStartObject = true;
            }
            while (true)
            {
                if (writeState.ReadingProperty)
                {
                    if (parser.Read() == false)
                    {
                        _objectState.Push(writeState);
                        return false;
                    }
                    if (state.CurrentTokenType == JsonParserToken.EndObject)
                        break;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        throw new InvalidOperationException("Property expected, but got " + state.CurrentTokenType);

                    if (writeState.FirstProperty == false)
                        WriteComma();

                    WriteString(context, state);
                    EnsureBuffer(1);
                    _buffer[_pos++] = Colon;

                    writeState.FirstProperty = false;
                    writeState.ReadingProperty = false;

                    if (parser.Read() == false)
                    {
                        _objectState.Push(writeState);
                        return false;
                    }
                }
               

                if (WriteValue(context, state, parser) == false)
                {
                    _objectState.Push(writeState);
                    return false;
                }
            }
            WriteEndObject();
            return true;
        }

        private bool WriteValue(RavenOperationContext context, JsonParserState state, IJsonParser parser)
        {
            switch (state.CurrentTokenType)
            {
                case JsonParserToken.Null:
                    WriteNull();
                    return true;
                case JsonParserToken.False:
                    WriteBool(false);
                    return true;
                case JsonParserToken.True:
                    WriteBool(true);
                    return true;
                case JsonParserToken.String:
                    WriteString(context, state);
                    return true;
                case JsonParserToken.Float:
                    WriteString(context, state);
                    return true;
                case JsonParserToken.Integer:
                    WriteInteger(state.Long);
                    return true;
                case JsonParserToken.StartObject:
                    return WriteObject(context, state, parser);
                case JsonParserToken.StartArray:
                    return WriteArray(context, state, parser);
                default:
                    throw new ArgumentOutOfRangeException("Could not understand " + state.CurrentTokenType);
            }
        }

        public bool WriteArray(RavenOperationContext context, JsonParserState state, IJsonParser parser)
        {
                WriteArrayState writeArrayState;
            if (_arrayState.Count != 0)
            {
                writeArrayState = _arrayState.Pop();
            }
            else
            {
                writeArrayState = new WriteArrayState
                {
                    FirstItem = true,
                    WriteStartArray = false,
                    ReadingItem = false
                };
            }
            if (writeArrayState.FirstItem && state.CurrentTokenType != JsonParserToken.StartArray)
                throw new InvalidOperationException("StartArray expected, but got " + state.CurrentTokenType);

            if (writeArrayState.WriteStartArray == false)
            {
                WriteStartArray();
                writeArrayState.WriteStartArray = true;
            }
            while (true)
            {
                if (writeArrayState.ReadingItem == false)
                {

                    if (parser.Read() == false)
                        return false;
                    if (state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (writeArrayState.FirstItem == false)
                        WriteComma();
                    writeArrayState.FirstItem = false;
                    writeArrayState.ReadingItem = true;
                }

                if (WriteValue(context, state, parser) == false)
                    return false;

                writeArrayState.ReadingItem = false;
            }
            WriteEndArray();
            return true;
        }

        public void Dispose()
        {
            Flush();
        }
    }
}