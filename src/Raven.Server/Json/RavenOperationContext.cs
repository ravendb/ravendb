using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json.Parsing;
using Sparrow;
using Sparrow.Binary;
using Voron;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class RavenOperationContext : IDisposable
    {
        private Stack<UnmanagedBuffersPool.AllocatedMemoryData>[] _allocatedMemory;

        public readonly UnmanagedBuffersPool Pool;
        private UnmanagedBuffersPool.AllocatedMemoryData _tempBuffer;
        private Dictionary<string, LazyStringValue> _fieldNames;
        private Dictionary<LazyStringValue, LazyStringValue> _internedFieldNames;
        private Dictionary<string, byte[]> _fieldNamesAsByteArrays;
        private bool _disposed;

        private byte[] _managedBuffer;
        private byte[] _parsingBuffer;
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        public LZ4 Lz4 = new LZ4();
        public UTF8Encoding Encoding;
        public Transaction Transaction;
        public CachedProperties CachedProperties;
        public StorageEnvironment Environment;
        private int _lastStreamSize = 4096;


        public RavenOperationContext(UnmanagedBuffersPool pool)
        {
            Pool = pool;
            Encoding = new UTF8Encoding();
            CachedProperties = new CachedProperties(this);
        }

        private byte[] GetParsingBuffer()
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
        /// <param name="documentId"></param>
        public UnmanagedWriteBuffer GetStream(string documentId)
        {
            return new UnmanagedWriteBuffer(this, GetMemory(_lastStreamSize));
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            Reset();
            Lz4.Dispose();
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
            LazyStringValue value;
            if (_fieldNames == null)
                _fieldNames = new Dictionary<string, LazyStringValue>();

            if (_fieldNames.TryGetValue(field, out value))
                return value;

            value = GetLazyString(field);
            _fieldNames[field] = value;
            return value;
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
                var writer = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, state);
                try
                {
                    CachedProperties.NewDocument();
                    writer.ReadObject();
                    while (true)
                    {
                        var read = stream.Read(buffer, 0, buffer.Length);
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


        public async Task<BlittableJsonReaderObject[]> ParseMultipleDocuments(Stream stream, int count, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            var state = new JsonParserState();
            var returnedArray = new BlittableJsonReaderObject[count];
            var buffer = GetParsingBuffer();
            using (var parser = new UnmanagedJsonParser(this, state, "many/docs"))
            {
                for (int i = 0; i < count; i++)
                {
                    BlittableJsonReaderObject reader;
                    var writer = new BlittableJsonDocumentBuilder(this, mode, "many/docs", parser, state);
                    try
                    {
                        writer.ReadObject();
                        CachedProperties.NewDocument();
                        while (true)
                        {
                            var read = stream.Read(buffer, 0, buffer.Length);
                            if (read == 0)
                                throw new EndOfStreamException("Stream ended without reaching end of json content");
                            parser.SetBuffer(buffer, read);
                            if (writer.Read())
                                break;
                        }
                        writer.FinalizeDocument();
                        _disposables.Add(writer);
                        reader = writer.CreateReader();
                    }
                    catch (Exception)
                    {
                        writer.Dispose();
                        throw;
                    }
                    returnedArray[i] = reader;
                }
            }

            return returnedArray;
        }


        public void LastStreamSize(int sizeInBytes)
        {
            _lastStreamSize = Math.Max(_lastStreamSize, sizeInBytes);
        }

        public void Reset()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _disposables.Clear();

            Transaction?.Dispose();
        }


        public void Write(Stream stream, BlittableJsonReaderObject json)
        {
            using (var writer = new BlittableJsonTextWriter(this, stream))
            {
                writer.WriteToOrdered(json);
            }
        }


        public void WriteOrdered(Stream stream, BlittableJsonReaderObject json)
        {
            using (var writer = new BlittableJsonTextWriter(this, stream))
            {
                writer.WriteToOrdered(json);
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

                writer.WriteObject(this, state, parser);
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

                writer.WriteArray(this, state, parser);
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
                    if(writer.Read() == false)
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
    }
}
