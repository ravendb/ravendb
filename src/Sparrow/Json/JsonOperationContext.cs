using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class JsonOperationContext : IDisposable
    {
        private Stack<UnmanagedBuffersPool.AllocatedMemoryData>[] _allocatedMemory;

        public readonly UnmanagedBuffersPool Pool;
        private UnmanagedBuffersPool.AllocatedMemoryData _tempBuffer;
        private Dictionary<StringSegment, LazyStringValue> _fieldNames;
        private Dictionary<LazyStringValue, LazyStringValue> _internedFieldNames;
        private Dictionary<string, byte[]> _fieldNamesAsByteArrays;
        private bool _disposed;

        private byte[] _managedBuffer;
        private byte[] _parsingBuffer;
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        public LZ4 Lz4 = new LZ4();
        public UTF8Encoding Encoding;

        public CachedProperties CachedProperties;

        private int _lastStreamSize = 4096;

        public JsonOperationContext(UnmanagedBuffersPool pool)
        {
            Pool = pool;
            Encoding = new UTF8Encoding();
            CachedProperties = new CachedProperties(this);
        }

        public byte[] GetParsingBuffer()
        {
            if (_parsingBuffer == null)
                _parsingBuffer = new byte[4096];
            return _parsingBuffer;
        }
        public byte[] GetManagedBuffer()
        {
            if (_managedBuffer == null)
                _managedBuffer = new byte[4096];
            return _managedBuffer;
        }

        /// <summary>
        /// Returns memory buffer to work with, be aware, this buffer is not thread safe
        /// </summary>
        /// <param name="requestedSize"></param>
        /// <param name="actualSize"></param>
        /// <returns></returns>
        public unsafe byte* GetNativeTempBuffer(int requestedSize, out int actualSize)
        {
            if (requestedSize == 0)
                throw new ArgumentException(nameof(requestedSize));

            if (_tempBuffer == null)
            {
                _tempBuffer = GetMemory(requestedSize);
            }
            else if (requestedSize > _tempBuffer.SizeInBytes)
            {
                ReturnMemory(_tempBuffer);
                _tempBuffer = GetMemory(requestedSize);
            }

            actualSize = _tempBuffer.SizeInBytes;
            return (byte*)_tempBuffer.Address;
        }

        public UnmanagedBuffersPool.AllocatedMemoryData GetMemory(int requestedSize)
        {
            if (requestedSize == 0)
                return new UnmanagedBuffersPool.AllocatedMemoryData
                {
                    Address = IntPtr.Zero,
                    SizeInBytes = 0
                };

            var actualSize = Bits.NextPowerOf2(requestedSize);
            var index = UnmanagedBuffersPool.GetIndexFromSize(actualSize);
            if (index == -1)
            {
                return Pool.Allocate(requestedSize);
            }

            if (_allocatedMemory?[index] == null ||
                _allocatedMemory[index].Count == 0)
                return Pool.Allocate(actualSize);
            var last = _allocatedMemory[index].Pop();
            return last;
        }

        public void ReturnMemory(UnmanagedBuffersPool.AllocatedMemoryData buffer)
        {
            if (buffer.SizeInBytes == 0)
                return;

            if (_allocatedMemory == null)
                _allocatedMemory = new Stack<UnmanagedBuffersPool.AllocatedMemoryData>[32];
            var index = UnmanagedBuffersPool.GetIndexFromSize(buffer.SizeInBytes);
            if (index == -1)
            {
                Pool.Return(buffer);
                return;
            }
            if (_allocatedMemory[index] == null)
                _allocatedMemory[index] = new Stack<UnmanagedBuffersPool.AllocatedMemoryData>();
            _allocatedMemory[index].Push(buffer);
        }

        /// <summary>
        /// Generates new unmanaged stream. Should be disposed at the end of the usage.
        /// </summary>
        public UnmanagedWriteBuffer GetStream()
        {
            return new UnmanagedWriteBuffer(this, GetMemory(_lastStreamSize));
        }

        public virtual void Dispose()
        {
            if (_disposed)
                return;
            Reset();

            if (_allocatedMemory != null)
            {
                foreach (var stack in _allocatedMemory)
                {
                    if(stack == null)
                        continue;
                    while (stack.Count > 0)
                    {
                        var memoryData = stack.Pop();
                        Pool.Return(memoryData);
                    }
                }
            }

            if (_tempBuffer != null)
                Pool.Return(_tempBuffer);
            if (_fieldNames != null)
            {
                foreach (var kvp in _fieldNames.Values)
                {
                    Pool.Return(kvp.AllocatedMemoryData);
                }
            }
            if (_internedFieldNames != null)
            {
                foreach (var key in _internedFieldNames.Keys)
                {
                    Pool.Return(key.AllocatedMemoryData);

                }
            }
            _disposed = true;
        }

        public LazyStringValue GetLazyStringForFieldWithCaching(string field)
        {
            return GetLazyStringForFieldWithCaching(new StringSegment(field, 0, field.Length));
        }


        public LazyStringValue GetLazyStringForFieldWithCaching(StringSegment field)
        {
            LazyStringValue value;
            if (_fieldNames == null)
                _fieldNames = new Dictionary<StringSegment, LazyStringValue>();

            if (_fieldNames.TryGetValue(field, out value))
                return value;

            value = GetLazyString(field.Value);
            _fieldNames[field] = value;
            return value;
        }

        public unsafe LazyStringValue GetLazyString(DateTime field)
        {
            return GetLazyString(field.ToString("O"));
        }

        public unsafe LazyStringValue GetLazyString(string field)
        {
            var state = new JsonParserState();
            state.FindEscapePositionsIn(field);
            var maxByteCount = Encoding.GetMaxByteCount(field.Length);
            var memory = GetMemory(maxByteCount + state.GetEscapePositionsSize());
            try
            {
                fixed (char* pField = field)
                {
                    var address = (byte*)memory.Address;
                    var actualSize = Encoding.GetBytes(pField, field.Length, address, memory.SizeInBytes);
                    state.WriteEscapePositionsTo(address + actualSize);
                    return new LazyStringValue(field, address, actualSize, this)
                    {
                        AllocatedMemoryData = memory
                    };
                }
            }
            catch (Exception)
            {
                ReturnMemory(memory);
                throw;
            }
        }


        public unsafe LazyStringValue GetLazyString(char[] chars, int start, int count)
        {
            LazyStringValue value;

            var state = new JsonParserState();
            state.FindEscapePositionsIn(chars, start, count);
            var maxByteCount = Encoding.GetMaxByteCount(count);
            var memory = GetMemory(maxByteCount + state.GetEscapePositionsSize());
            try
            {
                fixed (char* pChars = chars)
                {
                    var address = (byte*)memory.Address;
                    var actualSize = Encoding.GetBytes(pChars + start, count, address, memory.SizeInBytes);
                    state.WriteEscapePositionsTo(address + actualSize);
                    value = new LazyStringValue(null, address, actualSize, this);
                }
            }
            catch (Exception)
            {
                ReturnMemory(memory);
                throw;
            }
            return value;
        }

        public unsafe LazyStringValue Intern(LazyStringValue val)
        {
            LazyStringValue value;
            if (_internedFieldNames == null)
                _internedFieldNames = new Dictionary<LazyStringValue, LazyStringValue>();

            if (_internedFieldNames.TryGetValue(val, out value))
                return value;

            var memory = GetMemory(val.Size);
            try
            {
                var address = (byte*)memory.Address;
                Memory.Copy(address, val.Buffer, val.Size);
                value = new LazyStringValue(null, address, val.Size, this)
                {
                    EscapePositions = val.EscapePositions,
                    AllocatedMemoryData = memory
                };
                _internedFieldNames[value] = value;
                return value;

            }
            catch (Exception)
            {
                ReturnMemory(memory);
                throw;
            }
        }

        public byte[] GetBytesForFieldName(string field)
        {
            if (_fieldNamesAsByteArrays == null)
                _fieldNamesAsByteArrays = new Dictionary<string, byte[]>();

            byte[] returnedByteArray;

            if (_fieldNamesAsByteArrays.TryGetValue(field, out returnedByteArray))
            {
                return returnedByteArray;
            }
            returnedByteArray = Encoding.GetBytes(field);
            _fieldNamesAsByteArrays.Add(field, returnedByteArray);
            return returnedByteArray;
        }

        public BlittableJsonReaderObject ReadForDisk(Stream stream, string documentId)
        {
            return ParseToMemory(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public Task<BlittableJsonReaderObject> ReadForDiskAsync(Stream stream, string documentId)
        {
            return ParseToMemoryAsync(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public Task<BlittableJsonReaderObject> ReadForMemoryAsync(Stream stream, string documentId)
        {
            return ParseToMemoryAsync(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
        }

        public BlittableJsonReaderObject ReadForMemory(Stream stream, string documentId)
        {
            return ParseToMemory(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
        }

        public BlittableJsonReaderObject ReadObject(DynamicJsonValue builder, string documentId,
            BlittableJsonDocumentBuilder.UsageMode mode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            return ReadObjectInternal(builder, documentId, mode);
        }

        public BlittableJsonReaderObject ReadObject(BlittableJsonReaderObject obj, string documentId,
         BlittableJsonDocumentBuilder.UsageMode mode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            return ReadObjectInternal(obj, documentId, mode);
        }

        private BlittableJsonReaderObject ReadObjectInternal(object builder, string documentId, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            var state = new JsonParserState();
            using (var parser = new ObjectJsonParser(state, builder, this))
            {
                var writer = new BlittableJsonDocumentBuilder(this, mode, documentId, parser, state);
                try
                {
                    CachedProperties.NewDocument();
                    writer.ReadObject();
                    if (writer.Read() == false)
                        throw new InvalidOperationException("Partial content in object json parser shouldn't happen");
                    writer.FinalizeDocument();
                    _disposables.Add(writer);
                    return writer.CreateReader();
                }
                catch (Exception)
                {
                    writer.Dispose();
                    throw;
                }
            }
        }

        public async Task<BlittableJsonReaderObject> ReadFromWebSocket(
            WebSocket webSocket, 
            string debugTag, 
            CancellationToken cancellationToken)
        {
            var jsonParserState = new JsonParserState();
            using (var parser = new UnmanagedJsonParser(this, jsonParserState, debugTag))
            {
                var buffer = new ArraySegment<byte>(GetManagedBuffer());

                var writer = new BlittableJsonDocumentBuilder(this,
                    BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState);

                writer.ReadObject();
                var result = await webSocket.ReceiveAsync(buffer, cancellationToken);

                if (result.MessageType == WebSocketMessageType.Close)
                    return null;

                parser.SetBuffer(buffer.Array, result.Count);
                while (writer.Read() == false)
                {
                    result = await webSocket.ReceiveAsync(buffer, cancellationToken);
                    parser.SetBuffer(buffer.Array, result.Count);
                }
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }


        public BlittableJsonReaderObject Read(Stream stream, string documentId)
        {
            var state = BlittableJsonDocumentBuilder.UsageMode.ToDisk;
            return ParseToMemory(stream, documentId, state);
        }

        private BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            var state = new JsonParserState();
            var buffer = GetParsingBuffer();
            using (var parser = new UnmanagedJsonParser(this, state, debugTag))
            {
                var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, state);
                try
                {
                    CachedProperties.NewDocument();
                    builder.ReadObject();
                    while (true)
                    {
                        var read = stream.Read(buffer, 0, buffer.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(buffer, read);
                        if (builder.Read())
                            break;
                    }
                    builder.FinalizeDocument();

                    _disposables.Add(builder);
                    return builder.CreateReader();
                }
                catch (Exception)
                {
                    builder.Dispose();
                    throw;
                }
            }
        }

        private async Task<BlittableJsonReaderObject> ParseToMemoryAsync(Stream stream, string documentId, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            var state = new JsonParserState();
            var buffer = GetParsingBuffer();
            using (var parser = new UnmanagedJsonParser(this, state, documentId))
            {
                var writer = new BlittableJsonDocumentBuilder(this, mode, documentId, parser, state);
                try
                {
                    CachedProperties.NewDocument();
                    writer.ReadObject();
                    while (true)
                    {
                        var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(buffer, read);
                        if (writer.Read())
                            break;
                    }
                    writer.FinalizeDocument();

                    _disposables.Add(writer);
                    return writer.CreateReader();
                }
                catch (Exception)
                {
                    writer.Dispose();
                    throw;
                }
            }
        }


        public async Task<BlittableJsonReaderArray> ParseArrayToMemoryAsync(Stream stream, string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode)
        {
            var state = new JsonParserState();
            var buffer = GetParsingBuffer();
            using (var parser = new UnmanagedJsonParser(this, state, debugTag))
            {
                var writer = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, state);
                try
                {
                    CachedProperties.NewDocument();
                    writer.ReadArray();
                    while (true)
                    {
                        var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(buffer, read);
                        if (writer.Read())
                            break;
                    }
                    writer.FinalizeDocument();
                    _disposables.Add(writer);
                    return writer.CreateArrayReader();
                }
                catch (Exception)
                {
                    writer.Dispose();
                    throw;
                }
            }
        }

        public MultiDocumentParser ParseMultiFrom(Stream stream)
        {
            return new MultiDocumentParser(this, stream);
        }

        public class MultiDocumentParser : IDisposable
        {
            private readonly JsonOperationContext _context;
            private readonly Stream _stream;
            private readonly JsonParserState _state;
            private readonly byte[] _buffer;
            private readonly UnmanagedJsonParser _parser;

            public MultiDocumentParser(JsonOperationContext context, Stream stream)
            {
                _context = context;
                _stream = stream;
                _state = new JsonParserState();
                _buffer = context.GetParsingBuffer();
                _parser = new UnmanagedJsonParser(context, _state, "parse/multi");
            }

            public BlittableJsonReaderObject ParseToMemory(string debugTag = null) => 
                Parse(BlittableJsonDocumentBuilder.UsageMode.None, debugTag);

            public Task<BlittableJsonReaderObject> ParseToMemoryAsync(string debugTag = null) => 
                ParseAsync(BlittableJsonDocumentBuilder.UsageMode.None, debugTag);

            public Task<int> ReadAsync(byte[] buffer, int offset, int count)
            {
                if (_parser.BufferOffset != _parser.BufferSize)
                    return Task.FromResult(_parser.ReadBuffer(buffer, offset, count));
                return _stream.ReadAsync(buffer, offset, count);
            }

            public int Read(byte[] buffer, int offset, int count)
            {
                if (_parser.BufferOffset != _parser.BufferSize)
                    return _parser.ReadBuffer(buffer, offset, count);
                return _stream.Read(buffer, offset, count);
            }

            public void ReadExactly(byte[] buffer, int offset, int count)
            {
                int internalOffset = 0;
                while (count != internalOffset)
                {
                    var read = Read(buffer, offset + internalOffset, count - internalOffset);
                    if (read == 0)
                        throw new EndOfStreamException();
                    internalOffset += read;
                }
            }

            public int ReadByte()
            {
                if (_parser.BufferOffset != _parser.BufferSize)
                    return _parser.ReadByte();
                return _stream.ReadByte();
            }

            public async Task<BlittableJsonReaderObject> ParseAsync(BlittableJsonDocumentBuilder.UsageMode mode, string debugTag)
            {
                var writer = new BlittableJsonDocumentBuilder(_context, mode, debugTag, _parser, _state);
                try
                {
                    writer.ReadObject();
                    _context.CachedProperties.NewDocument();
                    while (true)
                    {
                        if (_parser.BufferOffset == _parser.BufferSize)
                        {
                            var read = await _stream.ReadAsync(_buffer, 0, _buffer.Length);
                            if (read == 0)
                                throw new EndOfStreamException("Stream ended without reaching end of json content");
                            _parser.SetBuffer(_buffer, read);
                        }
                        else
                        {
                            _parser.SetBuffer(new ArraySegment<byte>(_buffer, _parser.BufferOffset, _parser.BufferSize));
                        }
                        if (writer.Read())
                            break;
                    }
                    writer.FinalizeDocument();
                    return writer.CreateReader();
                }
                catch (Exception)
                {
                    writer.Dispose();
                    throw;
                }
            }

            public BlittableJsonReaderObject Parse(BlittableJsonDocumentBuilder.UsageMode mode, string debugTag)
            {
                var writer = new BlittableJsonDocumentBuilder(_context, mode, debugTag, _parser, _state);
                try
                {
                    writer.ReadObject();
                    _context.CachedProperties.NewDocument();
                    while (true)
                    {
                        if (_parser.BufferOffset == _parser.BufferSize)
                        {
                            var read = _stream.Read(_buffer, 0, _buffer.Length);
                            if (read == 0)
                                throw new EndOfStreamException("Stream ended without reaching end of json content");
                            _parser.SetBuffer(_buffer, read);
                        }
                        else
                        {
                            _parser.SetBuffer(new ArraySegment<byte>(_buffer, _parser.BufferOffset, _parser.BufferSize));
                        }
                        if (writer.Read())
                            break;
                    }
                    writer.FinalizeDocument();
                    return writer.CreateReader();
                }
                catch (Exception)
                {
                    writer.Dispose();
                    throw;
                }
            }

            public void Dispose()
            {
                _parser?.Dispose();
            }
        }


        public void LastStreamSize(int sizeInBytes)
        {
            _lastStreamSize = Math.Max(_lastStreamSize, sizeInBytes);
        }

        public virtual void Reset()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _disposables.Clear();
        }

        public void Write(Stream stream, BlittableJsonReaderObject json)
        {
            using (var writer = new BlittableJsonTextWriter(this, stream))
            {
                writer.WriteObjectOrdered(json);
            }
        }

        public void Write(BlittableJsonTextWriter writer, BlittableJsonReaderObject json)
        {
            WriteInternal(writer, json);
        }

        private void WriteInternal(BlittableJsonTextWriter writer, object json)
        {
            var state = new JsonParserState();
            using (var parser = new ObjectJsonParser(state, json, this))
            {
                parser.Read();

                WriteObject(writer, state, parser);
            }
        }

        public void Write(BlittableJsonTextWriter writer, DynamicJsonValue json)
        {
            WriteInternal(writer, json);
        }

        public void Write(BlittableJsonTextWriter writer, DynamicJsonArray json)
        {
            var state = new JsonParserState();
            using (var parser = new ObjectJsonParser(state, json, this))
            {
                parser.Read();

                WriteArray(writer, state, parser);
            }
        }

        public BlittableJsonReaderObject ReadObjectWithExternalProperties(DynamicJsonValue obj, string debugTag)
        {
            var state = new JsonParserState();
            using (var parser = new ObjectJsonParser(state, obj, this))
            {
                var writer = new BlittableJsonDocumentBuilder(this, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, state);
                try
                {
                    writer.ReadObject();
                    if (writer.Read() == false)
                        throw new InvalidOperationException("Partial json content in object json parser shouldn't happen");
                    writer.FinalizeDocumentWithoutProperties(CachedProperties.Version);
                    _disposables.Add(writer);
                    return writer.CreateReader(CachedProperties);
                }
                catch (Exception)
                {
                    writer.Dispose();
                    throw;
                }
            }
        }

        public unsafe void WriteObject(BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            if (state.CurrentTokenType != JsonParserToken.StartObject)
                throw new InvalidOperationException("StartObject expected, but got " + state.CurrentTokenType);

            writer.WriteStartObject();
            bool first = true;
            while (true)
            {
                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");
                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                    throw new InvalidOperationException("Property expected, but got " + state.CurrentTokenType);

                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WritePropertyName(new LazyStringValue(null, state.StringBuffer, state.StringSize, this));

                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");

                WriteValue(writer, state, parser);
            }
            writer.WriteEndObject();
        }

        private unsafe void WriteValue(BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            switch (state.CurrentTokenType)
            {
                case JsonParserToken.Null:
                    writer.WriteNull();
                    break;
                case JsonParserToken.False:
                    writer.WriteBool(false);
                    break;
                case JsonParserToken.True:
                    writer.WriteBool(true);
                    break;
                case JsonParserToken.String:
                    if (state.CompressedSize.HasValue)
                    {
                        var lazyCompressedStringValue = new LazyCompressedStringValue(null, state.StringBuffer, 
                            state.StringSize, state.CompressedSize.Value, this);
                        writer.WriteString(lazyCompressedStringValue);
                    }
                    else
                    {
                        writer.WriteString(new LazyStringValue(null, state.StringBuffer, state.StringSize, this));
                    }
                    break;
                case JsonParserToken.Float:
                    writer.WriteString(new LazyStringValue(null, state.StringBuffer, state.StringSize, this));
                    break;
                case JsonParserToken.Integer:
                    writer.WriteInteger(state.Long);
                    break;
                case JsonParserToken.StartObject:
                    WriteObject(writer, state, parser);
                    break;
                case JsonParserToken.StartArray:
                    WriteArray(writer, state, parser);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Could not understand " + state.CurrentTokenType);
            }
        }

        public void WriteArray(BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            if (state.CurrentTokenType != JsonParserToken.StartArray)
                throw new InvalidOperationException("StartArray expected, but got " + state.CurrentTokenType);

            writer.WriteStartArray();
            bool first = true;
            while (true)
            {
                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                if (first == false)
                    writer.WriteComma();
                first = false;

                WriteValue(writer, state, parser);
            }
            writer.WriteEndArray();
        }
    }
}
