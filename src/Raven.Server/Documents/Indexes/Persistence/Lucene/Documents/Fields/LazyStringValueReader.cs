using System;
using System.IO;
using System.Text;

using Raven.Server.Indexing.Corax;
using Raven.Server.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public unsafe class LazyStringReader : IDisposable
    {
        private readonly MmapStream _mmapStream = new MmapStream(null, 0);
        private readonly StreamReader _reader;

        private StringBuilder _sb;
        private char[] _readBuffer;
        
        public LazyStringReader()
        {
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
            if (value == null)
                return null;

            _mmapStream.Set(value.Buffer, value.Size);
            _reader.DiscardBufferedData();

            if (_readBuffer == null)
                _readBuffer = new char[128];

            if (_sb == null)
                _sb = new StringBuilder();
            else
                _sb.Clear();

            var read = 0;

            do
            {
                read = _reader.ReadBlock(_readBuffer, 0, _readBuffer.Length);
                _sb.Append(_readBuffer, 0, read);

            } while (read == _readBuffer.Length);

            return _sb.ToString();
        }

        public void Dispose()
        {
            _reader.Dispose();
            _mmapStream.Dispose();
        }
    }
}