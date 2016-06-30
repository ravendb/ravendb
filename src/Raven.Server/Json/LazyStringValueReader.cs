using System;
using System.IO;
using System.Text;

using Raven.Server.Indexing;

using Sparrow.Json;

namespace Raven.Server.Json
{
    public unsafe class LazyStringReader : IDisposable
    {
        private readonly MmapStream _mmapStream = new MmapStream(null, 0);
        private readonly LazyStringStreamReader _reader;

        private StringBuilder _sb;
        private char[] _readBuffer;

        public LazyStringReader()
        {
            _reader = new LazyStringStreamReader(_mmapStream, Encoding.UTF8);
        }

        public TextReader GetTextReaderFor(LazyStringValue value)
        {
            _reader.DiscardBufferedData();
            _mmapStream.Set(value.Buffer, value.Size);

            return _reader;
        }

        public string GetStringFor(LazyStringValue value)
        {
            if (value == null)
                return null;

            _reader.DiscardBufferedData();
            _mmapStream.Set(value.Buffer, value.Size);

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
            _reader.ForceDispose();
            _mmapStream.Dispose();
        }

        private class LazyStringStreamReader : StreamReader
        {
            public LazyStringStreamReader(Stream stream, Encoding encoding)
                : base(stream, encoding)
            {
            }

            protected override void Dispose(bool disposing)
            {
            }

            public void ForceDispose()
            {
                base.Dispose(true);
            }
        }
    }
}