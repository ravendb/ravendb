using System;
using System.IO;
using System.Text;
using Raven.Server.Indexing.Corax;
using Raven.Server.Json;
using Sparrow.Binary;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene.Documents.Fields
{
    public unsafe class LazyStringReader : IDisposable
    {
        private readonly MmapStream _mmapStream;
        private readonly StreamReader _reader;
        private char[] _buffer;

        public LazyStringReader()
        {
            _mmapStream = new MmapStream(null, 0);
            _reader = new StreamReader(_mmapStream, new UTF8Encoding()); // TODO arek - enconding should be the same as index's RavenOperationContext has
        }

        public TextReader GetTextReaderFor(LazyStringValue value)
        {
            _mmapStream.Set(value.Buffer, value.Size);
            _reader.DiscardBufferedData();

            return _reader;
        }

        public string GetStringFor(LazyStringValue value)
        {
            _mmapStream.Set(value.Buffer, value.Size);
            _reader.DiscardBufferedData();

            if (_buffer == null || _buffer.Length < value.Size)
                _buffer = new char[Bits.NextPowerOf2(value.Size)]; // TODO arek - should we take it from RavenOperationContext ?

            _reader.ReadBlock(_buffer, 0, value.Size);

            return new string(_buffer, 0, value.Size);
        }

        public void Dispose()
        {
            _reader.Dispose();
            _mmapStream.Dispose();
        }
    }
}