using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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

        private readonly Dictionary<string, LazyStringValue> _fieldNames =
            new Dictionary<string, LazyStringValue>(StringComparer.Ordinal);

        private bool _disposed;

        public unsafe class ManagedPinnedBuffer : IDisposable
        {
            public readonly ArraySegment<byte> Buffer;
            public readonly int Length;
            public readonly byte* Pointer;
            private GCHandle? _handle;

            public ManagedPinnedBuffer(ArraySegment<byte> buffer, byte* pointer)
            {
                Buffer = buffer;
                Length = buffer.Count;
                Pointer = pointer;
            }

            public static void Add(Stack<ManagedPinnedBuffer> stack)
            {
                var buffer = new byte[1024 * 128]; // making sure that this is on the LOH
                var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                try
                {
                    var ptr = (byte*)handle.AddrOfPinnedObject();
                    stack.Push(new ManagedPinnedBuffer(new ArraySegment<byte>(buffer, 0, 32 * 1024), ptr));
                    stack.Push(new ManagedPinnedBuffer(new ArraySegment<byte>(buffer, 32 * 1024, 32 * 1024), ptr + 32 * 1024));
                    stack.Push(new ManagedPinnedBuffer(new ArraySegment<byte>(buffer, 64 * 1024, 32 * 1024), ptr + 64 * 1024));
                    stack.Push(new ManagedPinnedBuffer(new ArraySegment<byte>(buffer, 96 * 1024, 32 * 1024), ptr + 96 * 1024) { _handle = handle });
                }
                catch (Exception)
                {
                    handle.Free();
                }
            }

            public void Dispose()
            {
                GC.SuppressFinalize(this);
                _handle?.Free();
                _handle = null;
            }

            ~ManagedPinnedBuffer()
            {
                Dispose();
            }
        }

        private Stack<ManagedPinnedBuffer> _managedBuffers;

        private readonly LinkedList<BlittableJsonReaderObject> _liveReaders =
            new LinkedList<BlittableJsonReaderObject>();

        public LZ4 Lz4 = new LZ4();
        public UTF8Encoding Encoding;

        public CachedProperties CachedProperties;

        internal DateTime InPoolSince;
        internal int InUse;
        private readonly JsonParserState _jsonParserState;
        private readonly ObjectJsonParser _objectJsonParser;
        private readonly BlittableJsonDocumentBuilder _writer;

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

            _jsonParserState = new JsonParserState();
            _objectJsonParser = new ObjectJsonParser(_jsonParserState, this);
            _writer = new BlittableJsonDocumentBuilder(this, _jsonParserState, _objectJsonParser);
        }

        public ReturnBuffer GetManagedBuffer(out ManagedPinnedBuffer buffer)
        {
            if (_managedBuffers == null)
                _managedBuffers = new Stack<ManagedPinnedBuffer>();
            if (_managedBuffers.Count == 0)
                ManagedPinnedBuffer.Add(_managedBuffers);

            buffer = _managedBuffers.Pop();
            return new ReturnBuffer(buffer, this);
        }

        public struct ReturnBuffer : IDisposable
        {
            private ManagedPinnedBuffer _buffer;
            private readonly JsonOperationContext _parent;

            public ReturnBuffer(ManagedPinnedBuffer buffer, JsonOperationContext parent)
            {
                _buffer = buffer;
                _parent = parent;
            }

            public void Dispose()
            {
                if (_buffer == null)
                    return;
                _parent._managedBuffers.Push(_buffer);
                _buffer = null;
            }
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

            return longLived
                ? _arenaAllocatorForLongLivedValues.Allocate(requestedSize)
                : _arenaAllocator.Allocate(requestedSize);
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

            _objectJsonParser.Dispose();
            _writer.Dispose();
            _arenaAllocator.Dispose();
            _arenaAllocatorForLongLivedValues?.Dispose();

            if (_managedBuffers != null)
            {
                foreach (var managedPinnedBuffer in _managedBuffers)
                {
                    managedPinnedBuffer.Dispose();
                }
                _managedBuffers = null;
            }


            _disposed = true;
        }

        public LazyStringValue GetLazyStringForFieldWithCaching(string field)
        {
            LazyStringValue value;

            if (_fieldNames.TryGetValue(field, out value))
                return value;

            var key = new StringSegment(field, 0, field.Length);
            value = GetLazyString(key, longLived: true);
            _fieldNames[field] = value;
            return value;
        }

        public LazyStringValue GetLazyString(string field)
        {
            if (field == null)
                return null;

            return GetLazyString(field, longLived: false);
        }

        private unsafe LazyStringValue GetLazyString(StringSegment field, bool longLived)
        {
            var state = new JsonParserState();
            state.FindEscapePositionsIn(field);
            var maxByteCount = Encoding.GetMaxByteCount(field.Length);
            var memory = GetMemory(maxByteCount + state.GetEscapePositionsSize(), longLived: longLived);

            fixed (char* pField = field.String)
            {
                var address = memory.Address;
                var actualSize = Encoding.GetBytes(pField + field.Start, field.Length, address, memory.SizeInBytes);
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

        private BlittableJsonReaderObject ReadObjectInternal(object builder, string documentId,
            BlittableJsonDocumentBuilder.UsageMode mode)
        {
            _jsonParserState.Reset();
            _objectJsonParser.Reset(builder);
            _writer.Reset(documentId, mode);
            CachedProperties.NewDocument();
            _writer.ReadObject();
            if (_writer.Read() == false)
                throw new InvalidOperationException("Partial content in object json parser shouldn't happen");
            _writer.FinalizeDocument();
            var reader = _writer.CreateReader();
            reader.DisposeTrackingReference = _liveReaders.AddFirst(reader);
            return reader;
        }

        public async Task<BlittableJsonReaderObject> ReadFromWebSocket(
            WebSocket webSocket,
            string debugTag,
            CancellationToken cancellationToken)
        {
            _jsonParserState.Reset();
            ManagedPinnedBuffer bytes;
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (GetManagedBuffer(out bytes))
            {
                var writer = new BlittableJsonDocumentBuilder(this,
                    BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, _jsonParserState);
                try
                {
                    writer.ReadObject();
                    var result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken);

                    if (result.MessageType == WebSocketMessageType.Close)
                        return null;

                    parser.SetBuffer(new ArraySegment<byte>(bytes.Buffer.Array, bytes.Buffer.Offset, result.Count));
                    while (writer.Read() == false)
                    {
                        result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken);
                        parser.SetBuffer(new ArraySegment<byte>(bytes.Buffer.Array, bytes.Buffer.Offset, result.Count));
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
        }


        public BlittableJsonReaderObject Read(Stream stream, string documentId)
        {
            var state = BlittableJsonDocumentBuilder.UsageMode.ToDisk;
            return ParseToMemory(stream, documentId, state);
        }

        private BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            _jsonParserState.Reset();
            ManagedPinnedBuffer bytes;
            using (GetManagedBuffer(out bytes))
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            {
                var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState);
                try
                {
                    CachedProperties.NewDocument();
                    builder.ReadObject();
                    while (true)
                    {
                        var read = stream.Read(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(new ArraySegment<byte>(bytes.Buffer.Array, bytes.Buffer.Offset, read));
                        if (builder.Read())
                            break;
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();
                    reader.DisposeTrackingReference = _liveReaders.AddFirst(reader);
                    return reader;
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
            _jsonParserState.Reset();
            ManagedPinnedBuffer bytes;
            using (GetManagedBuffer(out bytes))
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, documentId))
            {
                var writer = new BlittableJsonDocumentBuilder(this, mode, documentId, parser, _jsonParserState);
                try
                {
                    CachedProperties.NewDocument();
                    writer.ReadObject();
                    while (true)
                    {
                        var read = await stream.ReadAsync(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(new ArraySegment<byte>(bytes.Buffer.Array, bytes.Buffer.Offset, read));
                        if (writer.Read())
                            break;
                    }
                    writer.FinalizeDocument();

                    var reader = writer.CreateReader();
                    reader.DisposeTrackingReference = _liveReaders.AddFirst(reader);

                    return reader;
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
            _jsonParserState.Reset();
            ManagedPinnedBuffer bytes;
            using (GetManagedBuffer(out bytes))
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            {
                var writer = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState);
                try
                {
                    CachedProperties.NewDocument();
                    writer.ReadArray();
                    while (true)
                    {
                        var read = await stream.ReadAsync(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(new ArraySegment<byte>(bytes.Buffer.Array, bytes.Buffer.Offset, read));
                        if (writer.Read())
                            break;
                    }
                    writer.FinalizeDocument();
                    // here we "leak" the memory used by the array, in practice this is used
                    // in short scoped context, so we don't care
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
            private readonly ManagedPinnedBuffer _buffer;
            private readonly UnmanagedJsonParser _parser;
            private readonly BlittableJsonDocumentBuilder _writer;
            private ReturnBuffer _returnManagedBuffer;

            public MultiDocumentParser(JsonOperationContext context, Stream stream)
            {
                _context = context;
                _stream = stream;
                var state = new JsonParserState();
                _returnManagedBuffer = context.GetManagedBuffer(out _buffer);
                _parser = new UnmanagedJsonParser(context, state, "parse/multi");
                _writer = new BlittableJsonDocumentBuilder(_context, state, _parser);
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
                _writer.Reset(debugTag, mode);
                _parser.NewDocument();
                _writer.ReadObject();
                _context.CachedProperties.NewDocument();
                while (true)
                {
                    if (_parser.BufferOffset == _parser.BufferSize)
                    {
                        var read = await _stream.ReadAsync(_buffer.Buffer.Array, _buffer.Buffer.Offset, _buffer.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        _parser.SetBuffer(new ArraySegment<byte>(_buffer.Buffer.Array, _buffer.Buffer.Offset, read));
                    }
                    else
                    {
                        _parser.SetBuffer(new ArraySegment<byte>(_buffer.Buffer.Array, _buffer.Buffer.Offset + _parser.BufferOffset, _parser.BufferSize - _parser.BufferOffset));
                    }
                    if (_writer.Read())
                        break;
                }
                _writer.FinalizeDocument();
                return _writer.CreateReader();
            }

            public BlittableJsonReaderObject Parse(BlittableJsonDocumentBuilder.UsageMode mode, string debugTag)
            {
                _writer.Reset(debugTag, mode);
                _writer.ReadObject();
                _context.CachedProperties.NewDocument();
                while (true)
                {
                    if (_parser.BufferOffset == _parser.BufferSize)
                    {
                        var read = _stream.Read(_buffer.Buffer.Array, _buffer.Buffer.Offset, _buffer.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        _parser.SetBuffer(new ArraySegment<byte>(_buffer.Buffer.Array, _buffer.Buffer.Offset, read));
                    }
                    else
                    {
                        _parser.SetBuffer(new ArraySegment<byte>(_buffer.Buffer.Array, _buffer.Buffer.Offset + _parser.BufferOffset, _parser.BufferSize - _parser.BufferOffset));
                    }
                    if (_writer.Read())
                        break;
                }
                _writer.FinalizeDocument();
                return _writer.CreateReader();
            }

            public void Dispose()
            {
                _returnManagedBuffer.Dispose();
                _parser?.Dispose();
                _writer?.Dispose();
            }
        }

        internal void ReaderDisposed(LinkedListNode<BlittableJsonReaderObject> disposedNode)
        {
            if (disposedNode.List == _liveReaders)
                _liveReaders.Remove(disposedNode);
        }

        public virtual void ResetAndRenew()
        {
            Reset();
            Renew();
        }

        protected internal virtual void Renew()
        {
            _arenaAllocator.RenewArena();
            if (_arenaAllocatorForLongLivedValues == null)
            {
                _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator(_longLivedSize);
                CachedProperties = new CachedProperties(this);
            }
        }

        protected internal virtual unsafe void Reset()
        {
            if (_tempBuffer != null)
                _tempBuffer.Address = null;

            foreach (var builder in _liveReaders)
            {
                builder.DisposeTrackingReference = null;
                builder.Dispose();
            }

            _liveReaders.Clear();
            _arenaAllocator.ResetArena();

            if (_tempBuffer != null)
                GetNativeTempBuffer(_tempBuffer.SizeInBytes);

            // We don't reset _arenaAllocatorForLongLivedValues. It's used as a cache buffer for long lived strings like field names.
            // When a context is re-used, the buffer containing those field names was not reset and the strings are still valid and alive.

            if (_arenaAllocatorForLongLivedValues.Allocated > _initialSize)
            {
                // at this point, the long lived section is far too large, this is something that can happen
                // if we have dynamic properties. A back of the envelope calculation gives us roughly 32K 
                // property names before this kicks in, which is a true abuse of the system. In this case, 
                // in order to avoid unlimited growth, we'll reset the long lived section
                _arenaAllocatorForLongLivedValues.Dispose();
                _arenaAllocatorForLongLivedValues = null;
                _fieldNames.Clear();
                CachedProperties = null; // need to release this so can be collected
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
            _jsonParserState.Reset();
            _objectJsonParser.Reset(json);

            _objectJsonParser.Read();

            WriteObject(writer, _jsonParserState, _objectJsonParser);
        }

        public void Write(BlittableJsonTextWriter writer, DynamicJsonValue json)
        {
            WriteInternal(writer, json);
        }

        public void Write(BlittableJsonTextWriter writer, DynamicJsonArray json)
        {
            _jsonParserState.Reset();
            _objectJsonParser.Reset(json);

            _objectJsonParser.Read();

            WriteArray(writer, _jsonParserState, _objectJsonParser);
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
