using System;
using System.IO;
using System.Text;

using Raven.Server.Indexing;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server.Utils;

namespace Raven.Server.Json
{
    public sealed unsafe class LazyStringReader : IDisposable
    {
        internal const int MinLengthForLazyStringStreamReader = 2048;

        private MmapStream _mmapStream;
        private LazyStringStreamReader _reader;

        public TextReader GetTextReaderFor(LazyStringValue value)
        {
            // if the value is small, we don't want to create a reader for it
            // the reason is that a reader takes 3KB of memory, and if we won't
            // save it, might as well reduce the cost

            if (value.Length < MinLengthForLazyStringStreamReader && _reader == null)
                return new ReusableStringReader(GetStringFor(value));

            return GetTextReader(value.Buffer, value.Length);
        }

        public TextReader GetTextReader(byte* buffer, int len)
        {
            _mmapStream ??= new MmapStream(null, 0);
            _reader ??= new LazyStringStreamReader(_mmapStream, Encodings.Utf8);

            _reader.DiscardBufferedData();
            _mmapStream.Set(buffer, len);

            return _reader;
        }

        public static string GetStringFor(LazyStringValue value)
        {
            if (value == null)
                return null;

            return Encodings.Utf8.GetString(value.Buffer, value.Size);
        }

        public void Dispose()
        {
            _reader?.ForceDispose();
            _mmapStream?.Dispose();
        }

        private sealed class LazyStringStreamReader : StreamReader
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
