using System;
using System.IO;
using System.Threading.Tasks;

using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Sparrow;

namespace Raven.Server.Json
{
    public class AsyncBlittableJsonTextWriter
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

        public AsyncBlittableJsonTextWriter(MemoryOperationContext context, Stream stream)
        {
            _stream = stream;
            _buffer = context.GetManagedBuffer();
        }

        public async Task WriteToOrdered(BlittableJsonReaderObject obj)
        {
            await WriteStartObject();
            var props = obj.GetPropertiesByInsertionOrder();
            for (int i = 0; i < props.Length; i++)
            {
                if (i != 0)
                {
                    await WriteComma();
                }

                var prop = obj.GetPropertyByIndex(props[i]);
                await WritePropertyName(prop.Item1);

                await WriteValue(prop.Item3 & BlittableJsonReaderBase.TypesMask, prop.Item2, originalPropertyOrder: true);
            }

            await WriteEndObject();
        }

        public async Task WriteTo(BlittableJsonReaderObject obj)
        {
            await WriteStartObject();
            for (int i = 0; i < obj.Count; i++)
            {
                if (i != 0)
                {
                    await WriteComma();
                }
                var prop = obj.GetPropertyByIndex(i);
                await WritePropertyName(prop.Item1);

                await WriteValue(prop.Item3 & BlittableJsonReaderBase.TypesMask, prop.Item2);
            }

            await WriteEndObject();
        }


        private async Task WriteArrayToStream(BlittableJsonReaderArray blittableArray, bool originalPropertyOrder)
        {
            await WriteStartArray();
            var length = blittableArray.Length;
            for (var i = 0; i < length; i++)
            {
                var propertyValueAndType = blittableArray.GetValueTokenTupleByIndex(i);

                if (i != 0)
                {
                    await WriteComma();
                }
                // write field value
                await WriteValue(propertyValueAndType.Item2, propertyValueAndType.Item1, originalPropertyOrder);

            }
            await WriteEndArray();
        }

        private async Task WriteValue(BlittableJsonToken token, object val, bool originalPropertyOrder = false)
        {
            switch (token)
            {
                case BlittableJsonToken.StartArray:
                    await WriteArrayToStream((BlittableJsonReaderArray)val, originalPropertyOrder);
                    break;
                case BlittableJsonToken.StartObject:
                    var blittableJsonReaderObject = ((BlittableJsonReaderObject)val);
                    if (originalPropertyOrder)
                        await WriteToOrdered(blittableJsonReaderObject);
                    else
                        await WriteTo(blittableJsonReaderObject);
                    break;
                case BlittableJsonToken.String:
                    await WriteString((LazyStringValue)val);
                    break;
                case BlittableJsonToken.CompressedString:
                    await WriteString(((LazyCompressedStringValue)val).ToLazyStringValue());
                    break;
                case BlittableJsonToken.Integer:
                    await WriteInteger((long)val);
                    break;
                case BlittableJsonToken.Float:
                    await WriteDouble((LazyDoubleValue)val);
                    break;
                case BlittableJsonToken.Boolean:
                    await WriteBool((bool)val);
                    break;
                case BlittableJsonToken.Null:
                    await WriteNull();
                    break;
                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }


        public async Task WriteString(LazyStringValue str)
        {
            var size = str.Size;
            var start = 0;
            await EnsureBuffer(1);
            _buffer[_pos++] = Quote;
            var escapeSequencePos = size;
            var numberOfEscapeSequences = ReadVariableSizeInt(str, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = ReadVariableSizeInt(str, ref escapeSequencePos);
                await WriteRawString(str, start, bytesToSkip);
                start += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = str[start];
                start++;
                await EnsureBuffer(2);
                _buffer[_pos++] = (byte)'\\';
                _buffer[_pos++] = GetEscapeCharacter(b);
            }
            // write remaining (or full string) to the buffer in one shot
            await WriteRawString(str, start, size);

            await EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        private static unsafe int ReadVariableSizeInt(LazyStringValue str, ref int escapeSequencePos)
        {
            return BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
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


        private async Task WriteRawString(LazyStringValue str, int start, int count)
        {
            if (count < _buffer.Length)
            {
                await EnsureBuffer(count);
                CopyRawStringToBufferAfterEnsuringSizeFit(str, start, count);
                _pos += count;
                return;
            }

            // need to do this in pieces
            var posInStr = start;
            while (posInStr < count)
            {
                var amountToCopy = Math.Min(count - posInStr, _buffer.Length);
                await Flush();
                CopyRawStringToBufferAfterEnsuringSizeFit(str, posInStr, amountToCopy);
                posInStr += amountToCopy;
                _pos = amountToCopy;
            }
        }

        private unsafe void CopyRawStringToBufferAfterEnsuringSizeFit(LazyStringValue str, int start, int count)
        {
            fixed (byte* p = _buffer)
                Memory.Copy(p + _pos, str.Buffer + start, count);
        }

        public async Task WriteStartObject()
        {
            await EnsureBuffer(1);
            _buffer[_pos++] = StartObject;
        }

        public async Task WriteEndArray()
        {
            await EnsureBuffer(1);
            _buffer[_pos++] = EndArray;
        }

        public async Task WriteStartArray()
        {
            await EnsureBuffer(1);
            _buffer[_pos++] = StartArray;
        }

        public async Task WriteEndObject()
        {
            await EnsureBuffer(1);
            _buffer[_pos++] = EndObject;
        }


        private async Task EnsureBuffer(int len)
        {
            if (_pos + len < _buffer.Length)
                return;
            if (len >= _buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(len));

            await Flush();
        }

        public async Task Flush()
        {
            if (_pos == 0)
                return;
            await _stream.WriteAsync(_buffer, 0, _pos);
            _pos = 0;
        }

        public async Task WriteNull()
        {
            await EnsureBuffer(4);
            for (int i = 0; i < 4; i++)
            {
                _buffer[_pos++] = NullBuffer[i];
            }
        }

        public async Task WriteBool(bool val)
        {
            await EnsureBuffer(5);
            var buffer = val ? TrueBuffer : FalseBuffer;
            foreach (byte b in buffer)
            {
                _buffer[_pos++] = b;
            }
        }

        public async Task WriteComma()
        {
            await EnsureBuffer(1);
            _buffer[_pos++] = Comma;

        }

        public async Task WritePropertyName(LazyStringValue prop)
        {
            await WriteString(prop);
            await EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        public async Task WriteInteger(long val)
        {
            if (val == 0)
            {
                await EnsureBuffer(1);
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
                await EnsureBuffer(len + 1);
                _buffer[_pos++] = (byte)'-';
            }
            else
            {
                await EnsureBuffer(len);
            }
            for (int i = len - 1; i >= 0; i--)
            {
                _buffer[_pos + i] = (byte)('0' + Math.Abs(val % 10));
                val /= 10;
            }
            _pos += len;
        }

        public async Task WriteDouble(LazyDoubleValue val)
        {
            await WriteRawString(val.Inner, 0, val.Inner.Size);
        }
    }
}