using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public class BlittableObjectReader : IDisposable
    {
        private readonly NonDisposableStreamReader _reader;
        private readonly MemoryStream _ms;

        private StringBuilder _sb;
        private char[] _readBuffer;

        public int Capacity
        {
            get
            {
                if (_sb == null)
                    return _ms.Capacity;

                return _ms.Capacity + _sb.Capacity;
            }
        }

        public BlittableObjectReader()
        {
            _ms = new MemoryStream();
            _reader = new NonDisposableStreamReader(new StreamReader(_ms, Encodings.Utf8, true, 1024, leaveOpen: true));
        }

        public TextReader GetTextReaderFor(BlittableJsonReaderObject value)
        {
            _reader.InnerReader.DiscardBufferedData();

            var ms = _reader.InnerReader.BaseStream;

            ms.Position = 0;
            value._context.Sync.Write(ms, value);
            ms.SetLength(ms.Position);
            ms.Position = 0;

            return _reader;
        }

        public string GetStringFor(BlittableJsonReaderObject value)
        {
            if (value == null)
                return null;

            GetTextReaderFor(value);

            if (_readBuffer == null)
                _readBuffer = new char[128];

            if (_sb == null)
                _sb = new StringBuilder();
            else
                _sb.Clear();

            int read;

            do
            {
                read = _reader.InnerReader.ReadBlock(_readBuffer, 0, _readBuffer.Length);
                _sb.Append(_readBuffer, 0, read);
            } while (read == _readBuffer.Length);

            return _sb.ToString();
        }

        public void ResetCapacity()
        {
            _reader.InnerReader.DiscardBufferedData();

            if (_sb != null)
            {
                _sb.Clear();
                _sb.Capacity = 16;
            }

            _ms.SetLength(0);
            _ms.Capacity = 64;
        }

        public void Dispose()
        {
            _reader?.TrulyDispose();
            _ms?.Dispose();
        }

        private class NonDisposableStreamReader : TextReader
        {
            public readonly StreamReader InnerReader;

            public NonDisposableStreamReader(StreamReader reader)
            {
                InnerReader = reader ?? throw new ArgumentNullException(nameof(reader));
            }

            public override void Close()
            {
                // do nothing
            }

            protected override void Dispose(bool disposing)
            {
                // do nothing
            }

            public override int Peek()
            {
                return InnerReader.Peek();
            }

            public override int Read()
            {
                return InnerReader.Read();
            }

            public override int Read(char[] buffer, int index, int count)
            {
                return InnerReader.Read(buffer, index, count);
            }

            public override int Read(Span<char> buffer)
            {
                return InnerReader.Read(buffer);
            }

            public override Task<int> ReadAsync(char[] buffer, int index, int count)
            {
                return InnerReader.ReadAsync(buffer, index, count);
            }

            public override ValueTask<int> ReadAsync(Memory<char> buffer, CancellationToken cancellationToken = new CancellationToken())
            {
                return InnerReader.ReadAsync(buffer, cancellationToken);
            }

            public override int ReadBlock(char[] buffer, int index, int count)
            {
                return InnerReader.ReadBlock(buffer, index, count);
            }

            public override int ReadBlock(Span<char> buffer)
            {
                return InnerReader.ReadBlock(buffer);
            }

            public override Task<int> ReadBlockAsync(char[] buffer, int index, int count)
            {
                return InnerReader.ReadBlockAsync(buffer, index, count);
            }

            public override ValueTask<int> ReadBlockAsync(Memory<char> buffer, CancellationToken cancellationToken = new CancellationToken())
            {
                return InnerReader.ReadBlockAsync(buffer, cancellationToken);
            }

            public override string ReadLine()
            {
                return InnerReader.ReadLine();
            }

            public override Task<string> ReadLineAsync()
            {
                return InnerReader.ReadLineAsync();
            }

            public override string ReadToEnd()
            {
                return InnerReader.ReadToEnd();
            }

            public override Task<string> ReadToEndAsync()
            {
                return InnerReader.ReadToEndAsync();
            }

#pragma warning disable CS0672 // Member overrides obsolete member

            public override object InitializeLifetimeService()
#pragma warning restore CS0672 // Member overrides obsolete member
            {
#pragma warning disable SYSLIB0010 // Type or member is obsolete
                return InnerReader.InitializeLifetimeService();
#pragma warning restore SYSLIB0010 // Type or member is obsolete
            }

            public override bool Equals(object obj)
            {
                return InnerReader.Equals(obj);
            }

            public override int GetHashCode()
            {
                return InnerReader.GetHashCode();
            }

            public override string ToString()
            {
                return InnerReader.ToString();
            }

            public void TrulyDispose()
            {
                InnerReader?.Dispose();
            }
        }
    }
}
