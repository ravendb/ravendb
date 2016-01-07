using System;
using System.Collections.Generic;
using System.Text;
using ConsoleApplication4;
using NewBlittable;
using Raven.Imports.Newtonsoft.Json;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public unsafe class RavenOperationContext : IDisposable
    {
        private readonly UnmanagedBuffersPool _pool;
        private byte* _tempBuffer;
        private int _bufferSize;
        private readonly Dictionary<string, LazyStringValue> _fieldNames = new Dictionary<string, LazyStringValue>();
        private bool _disposed;

        public LZ4 Lz4 = new LZ4();
        public UTF8Encoding Encoding;
        public Transaction Transaction;
        private Dictionary<string,byte[]> _fieldNamesAsUnicodeByteArrays = new Dictionary<string, byte[]>();

        public RavenOperationContext(UnmanagedBuffersPool pool)
        {
            _pool = pool;
            Encoding = new UTF8Encoding();
            EncodingUnicode = new UnicodeEncoding();
        }


        /// <summary>
        /// Returns memory buffer to work with, be aware, this buffer is not thread safe
        /// </summary>
        /// <param name="requestedSize"></param>
        /// <param name="actualSize"></param>
        /// <returns></returns>
        public byte* GetTempBuffer(int requestedSize, out int actualSize)
        {
            if (requestedSize == 0)
                throw new ArgumentException(nameof(requestedSize));

            if (_bufferSize == 0)
            {
                _tempBuffer = _pool.GetMemory(requestedSize, string.Empty, out _bufferSize);
            }
            else if (requestedSize > _bufferSize)
            {
                _pool.ReturnMemory(_tempBuffer);
                _tempBuffer = _pool.GetMemory(requestedSize, string.Empty, out _bufferSize);
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
            return new UnmanagedWriteBuffer(_pool, documentId);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            Lz4.Dispose();
            if (_tempBuffer != null)
                _pool.ReturnMemory(_tempBuffer);
            foreach (var stringToByteComparable in _fieldNames.Values)
            {
                _pool.ReturnMemory(stringToByteComparable.Buffer);
            }
            _disposed = true;
        }

        public LazyStringValue GetComparerFor(string field)
        {
            LazyStringValue value;
            if (_fieldNames.TryGetValue(field, out value))
                return value;

            var maxByteCount = Encoding.GetMaxByteCount(field.Length);
            int actualSize;
            var memory = _pool.GetMemory(maxByteCount, field, out actualSize);
            fixed (char* pField = field)
            {
                actualSize = Encoding.GetBytes(pField, field.Length, memory, actualSize);
                _fieldNames[field] = value = new LazyStringValue(field, memory, actualSize, this);
            }
            return value;
        }

        public byte[] GetUnicodeByteArrayForFieldName(string field)
        {
            byte[] returnedByteArray = null;

            if (_fieldNamesAsUnicodeByteArrays.TryGetValue(field, out returnedByteArray))
            {
                return returnedByteArray;
            }
            returnedByteArray = EncodingUnicode.GetBytes(field);
            _fieldNamesAsUnicodeByteArrays.Add(field, returnedByteArray);
            return returnedByteArray;
        }

        public Encoding EncodingUnicode;

        public BlittableJsonWriter Read(JsonTextReader reader, string documentId)
        {
            var writer = new BlittableJsonWriter(reader, this, documentId);
            try
            {
                writer.Write();
                return writer;
            }
            catch (Exception e)
            {
                writer.Dispose();
                throw;
            }
        }

    }
}
