using System;
using System.IO;
using System.Text;

using Raven.Server.Indexing;

using Sparrow.Json;

namespace Raven.Server.Json
{
    public unsafe class LazyStringReader : IDisposable
    {
        private MmapStream _mmapStream;
        private LazyStringStreamReader _reader;

        public TextReader GetTextReaderFor(LazyStringValue value)
        {
            if (_mmapStream == null)
                _mmapStream = new MmapStream(null, 0);
            if(_reader == null)
                _reader = new LazyStringStreamReader(_mmapStream, Encoding.UTF8);

            _reader.DiscardBufferedData();
            _mmapStream.Set(value.Buffer, value.Size);

            return _reader;
        }

        public string GetStringFor(LazyStringValue value)
        {
            if (value == null)
                return null;

            return Encoding.UTF8.GetString(value.Buffer, value.Size);

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