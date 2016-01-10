using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
//using Raven.Imports.Newtonsoft.Json;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public unsafe class RavenOperationContext : IDisposable
    {
        public readonly UnmanagedBuffersPool Pool;
        private byte* _tempBuffer;
        private int _bufferSize;
        private Dictionary<string, LazyStringValue> _fieldNames;
        private Dictionary<string, byte[]> _fieldNamesAsByteArrays;
        private bool _disposed;

        private char[] _charBuffer;
        private byte[] _bytesBuffer;

        public LZ4 Lz4 = new LZ4();
        public UTF8Encoding Encoding;
        public Transaction Transaction;
        public CachedProperties CachedProperties;

        public RavenOperationContext(UnmanagedBuffersPool pool)
        {
            Pool = pool;
            Encoding = new UTF8Encoding();
            CachedProperties = new CachedProperties(this);
        }

        public char[] GetcharBuffer()
        {
            if(_charBuffer == null)
                _charBuffer = new char[128];
            return _charBuffer;
        }

        public byte[] GetManagedBuffer()
        {
            if(_bytesBuffer == null)
                _bytesBuffer = new byte[4096];
            return _bytesBuffer;
        }

        /// <summary>
        /// Returns memory buffer to work with, be aware, this buffer is not thread safe
        /// </summary>
        /// <param name="requestedSize"></param>
        /// <param name="actualSize"></param>
        /// <returns></returns>
        public byte* GetNativeTempBuffer(int requestedSize, out int actualSize)
        {
            if (requestedSize == 0)
                throw new ArgumentException(nameof(requestedSize));

            if (_bufferSize == 0)
            {
                _tempBuffer = Pool.GetMemory(requestedSize, out _bufferSize);
            }
            else if (requestedSize > _bufferSize)
            {
                Pool.ReturnMemory(_tempBuffer);
                _tempBuffer = Pool.GetMemory(requestedSize, out _bufferSize);
            }

            actualSize = _bufferSize;
            return _tempBuffer;
        }

        /// <summary>
        /// Generates new unmanaged stream. Should be disposed at the end of the usage.
        /// </summary>
        /// <param name="documentId"></param>
        public UnmanagedWriteBuffer GetStream(string documentId)
        {
            return new UnmanagedWriteBuffer(Pool);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            Lz4.Dispose();
            if (_tempBuffer != null)
                Pool.ReturnMemory(_tempBuffer);
            if (_fieldNames != null)
            {
                foreach (var stringToByteComparable in _fieldNames.Values)
                {
                    Pool.ReturnMemory(stringToByteComparable.Buffer);
                }
            }
            _disposed = true;
        }

        public LazyStringValue GetComparerFor(string field)
        {
            LazyStringValue value;
            if (_fieldNames == null)
                _fieldNames = new Dictionary<string, LazyStringValue>();

            if (_fieldNames.TryGetValue(field, out value))
                return value;

            var maxByteCount = Encoding.GetMaxByteCount(field.Length);
            int actualSize;
            var memory = Pool.GetMemory(maxByteCount, out actualSize);
            fixed (char* pField = field)
            {
                actualSize = Encoding.GetBytes(pField, field.Length, memory, actualSize);
                _fieldNames[field] = value = new LazyStringValue(field, memory, actualSize, this);
            }
            return value;
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

        public BlittableJsonWriter Read(JsonTextReader reader, string documentId)
        {
            var writer = new BlittableJsonWriter(reader, this, documentId);
            try
            {
                CachedProperties.NewDocument();
                writer.Run();
                return writer;
            }
            catch (Exception)
            {
                writer.Dispose();
                throw;
            }
        }

    }
}
