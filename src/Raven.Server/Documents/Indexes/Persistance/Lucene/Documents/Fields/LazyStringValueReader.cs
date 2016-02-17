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

        public LazyStringReader()
        {
            _mmapStream = new MmapStream(null, 0);
            _reader = new StreamReader(_mmapStream, Encoding.UTF8);
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

            return _reader.ReadToEnd();
        }

        public void Dispose()
        {
            _reader.Dispose();
            _mmapStream.Dispose();
        }
    }
}