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
        private const int InitialStreamSize = 4096;
        
        private readonly int _initialSize;
        private readonly int _longLivedSize;
        private readonly ArenaMemoryAllocator _arenaAllocator;
        private ArenaMemoryAllocator _arenaAllocatorForLongLivedValues;
        private AllocatedMemoryData _tempBuffer;
        private Dictionary<StringSegment, LazyStringValue> _fieldNames;
        private bool _disposed;

        private byte[] _managedBuffer;
        private byte[] _parsingBuffer;
        private readonly LinkedList<BlittableJsonDocumentBuilder> _liveBuilders = new LinkedList<BlittableJsonDocumentBuilder>();
        public LZ4 Lz4 = new LZ4();
        public UTF8Encoding Encoding;

        public CachedProperties CachedProperties;
        
        public static JsonOperationContext ShortTermSingleUse()
        {
            return new JsonOperationContext(4096, 1024);
        }

        public JsonOperationContext(int initialSize, int longLivedSize)
        {
            _initialSize = initialSize;
            _longLivedSize = longLivedSize;
            _arenaAllocator = new ArenaMemoryAllocator(initialSize);
            _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator(longLivedSize);
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
        /// <returns></returns>
        public unsafe byte* GetNativeTempBuffer(int requestedSize)
        {
            if (_tempBuffer == null ||
                _tempBuffer.Address == null ||
                _tempBuffer.SizeInBytes < requestedSize)
            {
                _tempBuffer = GetMemory(Math.Max(_tempBuffer?.SizeInBytes ?? 0, requestedSize));
            }

            return _tempBuffer.Address;
        }

        public AllocatedMemoryData GetMemory(int requestedSize)
        {
            return GetMemory(requestedSize, longLived: false);
        }

        private AllocatedMemoryData GetMemory(int requestedSize, bool longLived)
        {
            if (requestedSize <= 0)
                throw new ArgumentException(nameof(requestedSize));

            return longLived ?
                _arenaAllocatorForLongLivedValues.Allocate(requestedSize) :
                _arenaAllocator.Allocate(requestedSize);
        }

        /// <summary>
        /// Generates new unmanaged stream. Should be disposed at the end of the usage.
        /// </summary>
        public UnmanagedWriteBuffer GetStream()
        {
            return new UnmanagedWriteBuffer(this, GetMemory(InitialStreamSize));
        }

        public virtual void Dispose()
        {
            if (_disposed)
                return;

            Reset();

            _arenaAllocator.Dispose();
            _arenaAllocatorForLongLivedValues.Dispose();

            _disposed = true;
        }

        public LazyStringValue GetLazyStringForFieldWithCaching(string field)
        {
            var key = new StringSegment(field, 0, field.Length);
            LazyStringValue value;
            if (_fieldNames == null)
                _fieldNames = new Dictionary<StringSegment, LazyStringValue>();

            if (_fieldNames.TryGetValue(key, out value))
                return value;

            value = GetLazyString(field, longLived: true);
            _fieldNames[key] = value;
            return value;
        }

        public LazyStringValue GetLazyString(string field)
        {
            return GetLazyString(field, longLived: false);
        }

        private unsafe LazyStringValue GetLazyString(string field, bool longLived)
        {
            if (field == null)
                return null;

            var state = new JsonParserState();
            state.FindEscapePositionsIn(field);
            var maxByteCount = Encoding.GetMaxByteCount(field.Length);
            var memory = GetMemory(maxByteCount + state.GetEscapePositionsSize(), longLived: longLived);

            fixed (char* pField = field)
            {
                var address = (byte*)memory.Address;
                var actualSize = Encoding.GetBytes(pField, field.Length, address, memory.SizeInBytes);
                state.WriteEscapePositionsTo(address + actualSize);
                var result = new LazyStringValue(field, address, actualSize, this)
                {
                    AllocatedMemoryData = memory,
                };

                if (state.EscapePositions.Count > 0)
                {
                    result.EscapePositions = state.EscapePositions.ToArray();
                }
                return result;
            }
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
                    writer.DisposeTrackingReference = _liveBuilders.AddFirst(writer);
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

                    builder.DisposeTrackingReference = _liveBuilders.AddFirst(builder);
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

                    writer.DisposeTrackingReference = _liveBuilders.AddFirst(writer);
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
                    writer.DisposeTrackingReference = _liveBuilders.AddFirst(writer);
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
                    _parser.NewDocument();
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

        internal void BuilderDisposed(LinkedListNode<BlittableJsonDocumentBuilder> disposedNode)
        {
            if (disposedNode.List == _liveBuilders)
                _liveBuilders.Remove(disposedNode);
        }

        public virtual unsafe void Reset()
        {
            if (_tempBuffer != null)
                _tempBuffer.Address = null;

            foreach (var builder in _liveBuilders)
            {
                builder.DisposeTrackingReference = null;
                builder.Dispose();
            }

            _liveBuilders.Clear();
            _arenaAllocator.ResetArena();

            // We don't reset _arenaAllocatorForLongLivedValues. It's used as a cache buffer for long lived strings like field names.
            // When a context is re-used, the buffer containing those field names was not reset and the strings are still valid and alive.

            if (_arenaAllocatorForLongLivedValues.Allocated > _initialSize)
            {
                // at this point, the long lived section is far too large, this is something that can happen
                // if we have dynamic properties. A back of the envelope calculation gives us roughly 32K 
                // property names before this kicks in, which is a true abuse of the system. In this case, 
                // in order to avoid unlimited growth, we'll reset the long lived section
                _arenaAllocatorForLongLivedValues.Dispose();
                _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator(_longLivedSize);
                CachedProperties = new CachedProperties(this);// need to reset this as well
                _fieldNames.Clear();
            }
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

                var lazyStringValue = new LazyStringValue(null, state.StringBuffer, state.StringSize, this);
                writer.WritePropertyName(lazyStringValue);

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
                    writer.WriteDouble(new LazyDoubleValue(new LazyStringValue(null, state.StringBuffer, state.StringSize, this)));
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

        public bool GrowAllocation(AllocatedMemoryData allocation, int sizeIncrease)
        {
            return _arenaAllocator.GrowAllocation(allocation, sizeIncrease);
        }


        public void ReturnMemory(AllocatedMemoryData allocation)
        {
            _arenaAllocator.Return(allocation);
        }
    }
}
