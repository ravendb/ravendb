using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Sparrow.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class JsonOperationContext : IDisposable
    {
        private int _generation;
        private const int InitialStreamSize = 4096;
        private readonly int _initialSize;
        private readonly int _longLivedSize;
        private readonly ArenaMemoryAllocator _arenaAllocator;
        private ArenaMemoryAllocator _arenaAllocatorForLongLivedValues;
        private AllocatedMemoryData _tempBuffer;
        private List<GCHandle> _pinnedObjects;
   
        private readonly FastDictionary<string, LazyStringValue, OrdinalStringStructComparer> _fieldNames = new FastDictionary<string, LazyStringValue, OrdinalStringStructComparer>(OrdinalStringStructComparer.Instance);

        private readonly FastList<LazyStringValue> _allocateStringValues = new FastList<LazyStringValue>(256);
        private int _numberOfAllocatedStringsValues;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LazyStringValue AllocateStringValue(string str, byte* ptr, int size)
        {
            if (_numberOfAllocatedStringsValues < _allocateStringValues.Count)
            {
                var lazyStringValue = _allocateStringValues[_numberOfAllocatedStringsValues++];
                Debug.Assert(lazyStringValue != null);
                lazyStringValue.Renew(str,ptr, size);
                return lazyStringValue;
            }

            var allocateStringValue = new LazyStringValue(str, ptr, size, this);
            if (_numberOfAllocatedStringsValues < 25 * 1000)
            {
                _allocateStringValues.Add(allocateStringValue);
                _numberOfAllocatedStringsValues++;
            }
            return allocateStringValue;
        }

        private bool _disposed;

        public unsafe class ManagedPinnedBuffer : IDisposable
        {
            public readonly ArraySegment<byte> Buffer;
            public readonly int Length;
            public int Valid,Used;
            public readonly byte* Pointer;
            private GCHandle? _handle;

            public ManagedPinnedBuffer(ArraySegment<byte> buffer, byte* pointer)
            {
                Buffer = buffer;
                Length = buffer.Count;
                Pointer = pointer;
            }

            public static ManagedPinnedBuffer LongLivedInstance()
            {
                var buffer = new byte[1024 * 128]; // making sure that this is on the LOH
                var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                try
                {
                    var ptr = (byte*)handle.AddrOfPinnedObject();
                    return new ManagedPinnedBuffer(new ArraySegment<byte>(buffer), ptr) {_handle = handle};
                }
                catch (Exception)
                {
                    handle.Free();
                    throw;
                }
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

        public static readonly UTF8Encoding Encoding = new UTF8Encoding();

        public CachedProperties CachedProperties;

        internal DateTime InPoolSince;
        internal int InUse;
        private readonly JsonParserState _jsonParserState;
        private readonly ObjectJsonParser _objectJsonParser;
        private readonly BlittableJsonDocumentBuilder _documentBuilder;

        public int Generation => _generation;

        public long AllocatedMemory => _arenaAllocator.TotalUsed;

        protected readonly LowMemoryFlag LowMemoryFlag;

        public static JsonOperationContext ShortTermSingleUse()
        {
            return new JsonOperationContext(4096, 1024, LowMemoryFlag.None);
        }

        public JsonOperationContext(int initialSize, int longLivedSize, LowMemoryFlag lowMemoryFlag)
        {
            Debug.Assert(lowMemoryFlag != null);
            
            _initialSize = initialSize;
            _longLivedSize = longLivedSize;
            _arenaAllocator = new ArenaMemoryAllocator(lowMemoryFlag, initialSize);
            _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator(lowMemoryFlag, longLivedSize);
            CachedProperties = new CachedProperties(this);
            _jsonParserState = new JsonParserState();
            _objectJsonParser = new ObjectJsonParser(_jsonParserState, this);
            _documentBuilder = new BlittableJsonDocumentBuilder(this, _jsonParserState, _objectJsonParser);
            LowMemoryFlag = lowMemoryFlag;
#if MEM_GUARD_STACK
            ElectricFencedMemory.IncrementConext();
            ElectricFencedMemory.RegisterContextAllocation(this,Environment.StackTrace);
#endif
        }

        public ReturnBuffer GetManagedBuffer(out ManagedPinnedBuffer buffer)
        {
            if (_managedBuffers == null)
                _managedBuffers = new Stack<ManagedPinnedBuffer>();
            if (_managedBuffers.Count == 0)
                ManagedPinnedBuffer.Add(_managedBuffers);

            buffer = _managedBuffers.Pop();
            buffer.Valid = buffer.Used = 0;
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

                //_parent disposal sets _managedBuffers to null,
                //throwing ObjectDisposedException() to make it more visible
                if (_parent._disposed)
                    ThrowParentWasDisposed();

                _parent._managedBuffers.Push(_buffer);
                _buffer = null;
            }

            private static void ThrowParentWasDisposed()
            {
                throw new ObjectDisposedException(
                    "ReturnBuffer should not be disposed after it's parent operation context was disposed");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetMemory(int requestedSize)
        {
            return GetMemory(requestedSize, longLived: false);
        }

        private AllocatedMemoryData GetMemory(int requestedSize, bool longLived)
        {
            if (requestedSize <= 0)
                throw new ArgumentException(nameof(requestedSize));

            var allocatedMemory = longLived
                                        ? _arenaAllocatorForLongLivedValues.Allocate(requestedSize)
                                        : _arenaAllocator.Allocate(requestedSize);

            allocatedMemory.ContextGeneration = Generation;
            allocatedMemory.Parent = this;
#if DEBUG
            allocatedMemory.IsLongLived = longLived;
#endif
            return allocatedMemory;
        }

        /// <summary>
        /// Generates new unmanaged stream. Should be disposed at the end of the usage.
        /// </summary>
        public UnmanagedWriteBuffer GetStream()
        {
            var bufferMemory = GetMemory(InitialStreamSize);
            return new UnmanagedWriteBuffer(this, bufferMemory);
        }

        public virtual void Dispose()
        {
            if (_disposed)
                return;

#if MEM_GUARD_STACK
            ElectricFencedMemory.DecrementConext();
            ElectricFencedMemory.UnRegisterContextAllocation(this);
#endif

            Reset(true);

            _documentBuilder.Dispose();

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

            if (_pinnedObjects != null)
            {
                foreach (var pinnedObject in _pinnedObjects)
                {
                    pinnedObject.Free();
                }
            }

            _disposed = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue GetLazyStringForFieldWithCaching(StringSegment key)
        {
            LazyStringValue value;

            var field = key.Value;
            if (_fieldNames.TryGetValue(field, out value))
            {
                //sanity check, in case the 'value' is manually disposed outside of this function
                Debug.Assert(value.IsDisposed == false);
                return value;
            }

            return GetLazyStringForFieldWithCachingUnlikely(field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue GetLazyStringForFieldWithCaching(string field)
        {
            LazyStringValue value;

            if (_fieldNames.TryGetValue(field, out value))
            {
                // PERF: This is usually the most common scenario, so actually being contiguous improves the behavior.
                Debug.Assert(value.IsDisposed == false);
                return value;
            }

            return GetLazyStringForFieldWithCachingUnlikely(field);
        }

        private LazyStringValue GetLazyStringForFieldWithCachingUnlikely(StringSegment key)
        {
            LazyStringValue value = GetLazyString(key, longLived: true);
            _fieldNames[key] = value;

            //sanity check, in case the 'value' is manually disposed outside of this function
            Debug.Assert(value.IsDisposed == false);
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
            var maxByteCount = Encoding.GetMaxByteCount(field.Length);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(field);

            var memory = GetMemory(maxByteCount + escapePositionsSize, longLived: longLived);

            fixed (char* pField = field.String)
            {
                var address = memory.Address;
                var actualSize = Encoding.GetBytes(pField + field.Start, field.Length, address, memory.SizeInBytes);

                state.FindEscapePositionsIn(address, actualSize, escapePositionsSize);

                state.WriteEscapePositionsTo(address + actualSize);
                LazyStringValue result = longLived == false ? AllocateStringValue(field, address, actualSize) : new LazyStringValue(field, address, actualSize, this);
                result.AllocatedMemoryData = memory;

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

        public Task<BlittableJsonReaderObject> ReadForDiskAsync(Stream stream, string documentId, CancellationToken? token = null)
        {
            return ParseToMemoryAsync(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk, token);
        }

        public Task<BlittableJsonReaderObject> ReadForMemoryAsync(Stream stream, string documentId, CancellationToken? token = null)
        {
            return ParseToMemoryAsync(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None, token);
        }

        public BlittableJsonReaderObject ReadForMemory(Stream stream, string documentId)
        {
            return ParseToMemory(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
        }

        public BlittableJsonReaderObject ReadObject(DynamicJsonValue builder, string documentId,
            BlittableJsonDocumentBuilder.UsageMode mode = BlittableJsonDocumentBuilder.UsageMode.None, IBlittableDocumentModifier modifier = null)
        {
            return ReadObjectInternal(builder, documentId, mode, modifier);
        }

        public BlittableJsonReaderObject ReadObject(BlittableJsonReaderObject obj, string documentId,
            BlittableJsonDocumentBuilder.UsageMode mode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            return ReadObjectInternal(obj, documentId, mode);
        }

        private BlittableJsonReaderObject ReadObjectInternal(object builder, string documentId,
            BlittableJsonDocumentBuilder.UsageMode mode, IBlittableDocumentModifier modifier = null)
        {
            _jsonParserState.Reset();
            _objectJsonParser.Reset(builder);
            _documentBuilder.Renew(documentId, mode);
            CachedProperties.NewDocument();
            _documentBuilder._modifier = modifier;
            _documentBuilder.ReadObjectDocument();
            if (_documentBuilder.Read() == false)
                throw new InvalidOperationException("Partial content in object json parser shouldn't happen");
            _documentBuilder.FinalizeDocument();
            
            _objectJsonParser.Reset(null);

            var reader = _documentBuilder.CreateReader();
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
                var builder = new BlittableJsonDocumentBuilder(this,
                    BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, _jsonParserState);
                try
                {
                    builder.ReadObjectDocument();
                    var result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken);
                    
                    if (result.MessageType == WebSocketMessageType.Close)
                        return null;
                    bytes.Valid = result.Count;
                    bytes.Used = 0;

                    parser.SetBuffer(bytes);
                    while (true)
                    {
                        var read = builder.Read();
                        bytes.Used += parser.BufferOffset;
                        if (read)
                            break;
                        result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken);
                        bytes.Valid = result.Count;
                        bytes.Used = 0;
                        parser.SetBuffer(bytes);
                    }
                    builder.FinalizeDocument();
                    return builder.CreateReader();
                }
                catch (Exception)
                {
                    builder.Dispose();
                    throw;
                }
            }
        }

        public BlittableJsonReaderObject Read(Stream stream, string documentId, IBlittableDocumentModifier modifier = null)
        {
            var state = BlittableJsonDocumentBuilder.UsageMode.ToDisk;
            return ParseToMemory(stream, documentId, state, modifier);
        }

        private BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag, BlittableJsonDocumentBuilder.UsageMode mode, IBlittableDocumentModifier modifier = null)
        {
            ManagedPinnedBuffer bytes;
            using (GetManagedBuffer(out bytes))
            {
                return ParseToMemory(stream, debugTag, mode, bytes, modifier);
            }
        }

        public BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag, 
            BlittableJsonDocumentBuilder.UsageMode mode,
            ManagedPinnedBuffer bytes, IBlittableDocumentModifier modifier = null)
        {
            _jsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState, modifier: modifier))
            {
                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    if (bytes.Valid == bytes.Used)
                    {
                        var read = stream.Read(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read;
                        bytes.Used = 0;
                    }
                    parser.SetBuffer(bytes);
                    var result = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }

        public async Task<BlittableJsonReaderObject> ParseToMemoryAsync(WebSocket webSocket, string debugTag,
           BlittableJsonDocumentBuilder.UsageMode mode,
           ManagedPinnedBuffer bytes,
           CancellationToken token = default(CancellationToken)
           )
        {
            _jsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState))
            {
                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    if (bytes.Valid == bytes.Used)
                    {
                        var read = await webSocket.ReceiveAsync(bytes.Buffer, token);
                        if (read.Count == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read.Count;
                        bytes.Used = 0;
                    }
                    parser.SetBuffer(bytes);
                    var result = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }

        private async Task<BlittableJsonReaderObject> ParseToMemoryAsync(Stream stream, string documentId, BlittableJsonDocumentBuilder.UsageMode mode, CancellationToken? token = null)
        {
            using (GetManagedBuffer(out ManagedPinnedBuffer bytes))
                return await ParseToMemoryAsync(stream, documentId, mode, bytes, token);
        }

        public async Task<BlittableJsonReaderObject> ParseToMemoryAsync(Stream stream, string documentId, BlittableJsonDocumentBuilder.UsageMode mode, ManagedPinnedBuffer bytes, CancellationToken? token = null)
        {
            _jsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, documentId))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, documentId, parser, _jsonParserState))
            {
                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    token?.ThrowIfCancellationRequested();
                    if (bytes.Valid == bytes.Used)
                    { 
                        var read = token.HasValue ? 
                            await stream.ReadAsync(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length, token.Value) :
                            await stream.ReadAsync(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read;
                        bytes.Used = 0;
                    }
                    parser.SetBuffer(bytes);
                    var result = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();

                return reader;
            }
        }


        public async Task<Tuple<BlittableJsonReaderArray, IDisposable>> ParseArrayToMemoryAsync(Stream stream, string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode, bool noCache = false)
        {
            _jsonParserState.Reset();
            ManagedPinnedBuffer bytes;
            using (GetManagedBuffer(out bytes))
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState))
            {
                CachedProperties.NewDocument();
                builder.ReadArrayDocument();
                while (true)
                {
                    if (bytes.Valid == bytes.Used)
                    {
                        var read = await stream.ReadAsync(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read;
                        bytes.Used = 0;
                    }
                    parser.SetBuffer(bytes);
                    var readObj = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (readObj)
                        break;
                }
                builder.FinalizeDocument();
                var arrayReader = builder.CreateArrayReader(noCache);
                return Tuple.Create(arrayReader, (IDisposable) arrayReader.Parent);
            }
        }


        protected virtual void InternalResetAndRenew()
        {
            Reset();
            Renew();
        }

        protected internal virtual void Renew()
        {
            _arenaAllocator.RenewArena();
            if (_arenaAllocatorForLongLivedValues == null)
            {
                _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator(LowMemoryFlag, _longLivedSize);
                CachedProperties = new CachedProperties(this);
            }
        }

        protected internal virtual unsafe void Reset(bool forceReleaseLongLivedAllocator = false)
        {
            if (_tempBuffer != null && _tempBuffer.Address != null)
            {
                _arenaAllocator.Return(_tempBuffer);
                _tempBuffer = null;
            }

            _documentBuilder.Reset();

            // We don't reset _arenaAllocatorForLongLivedValues. It's used as a cache buffer for long lived strings like field names.
            // When a context is re-used, the buffer containing those field names was not reset and the strings are still valid and alive.

            var allocatorForLongLivedValues = _arenaAllocatorForLongLivedValues;
            if (allocatorForLongLivedValues != null && 
                (allocatorForLongLivedValues.Allocated > _initialSize || forceReleaseLongLivedAllocator))
            {
                foreach(var mem in _fieldNames.Values)
                {
                    _arenaAllocatorForLongLivedValues.Return(mem.AllocatedMemoryData);
                    mem.AllocatedMemoryData = null;
                    mem.Dispose();
                }        
                        
                _arenaAllocatorForLongLivedValues = null;

                // at this point, the long lived section is far too large, this is something that can happen
                // if we have dynamic properties. A back of the envelope calculation gives us roughly 32K 
                // property names before this kicks in, which is a true abuse of the system. In this case, 
                // in order to avoid unlimited growth, we'll reset the long lived section
                allocatorForLongLivedValues.Dispose();

                _fieldNames.Clear();
                CachedProperties = null; // need to release this so can be collected
            }
            _objectJsonParser.Reset(null);
            _arenaAllocator.ResetArena();
            _numberOfAllocatedStringsValues = 0;
            _generation = _generation + 1;
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

            _objectJsonParser.Reset(null);
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

            _objectJsonParser.Reset(null);
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

                var lazyStringValue = AllocateStringValue(null, state.StringBuffer, state.StringSize);
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
                        writer.WriteString(AllocateStringValue(null, state.StringBuffer, state.StringSize));
                    }
                    break;
                case JsonParserToken.Float:
                    writer.WriteDouble(new LazyDoubleValue(AllocateStringValue(null, state.StringBuffer, state.StringSize)));
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
            if (_generation != allocation.ContextGeneration)
                ThrowUseAfterFree();

            _arenaAllocator.Return(allocation);
        }

        private static void ThrowUseAfterFree()
        {
            throw new InvalidOperationException(
                "UseAfterFree detected! Attempt to return memory from previous generation, Reset has already been called and the memory reused!");
        }

        public IntPtr PinObjectAndGetAddress(object obj)
        {
            var handle = GCHandle.Alloc(obj, GCHandleType.Pinned);

            if (_pinnedObjects == null)
                _pinnedObjects = new List<GCHandle>();

            _pinnedObjects.Add(handle);

            return handle.AddrOfPinnedObject();
        }
    }
}
