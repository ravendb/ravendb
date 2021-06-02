using System;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Util;
using Raven.Server.Indexing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents
{
    public class StreamsTempFile : IDisposable
    {
        private readonly string _tempFile;
        private readonly StorageEnvironment _environment;
        internal readonly TempFileStream _file;
        internal bool _reading;
        private InnerStream _previousInstance;

        public StreamsTempFile(string tempFile, StorageEnvironment environment)
        {
            _tempFile = tempFile;
            _environment = environment;

            _file = new TempFileStream(SafeFileStream.Create(_tempFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose | FileOptions.SequentialScan));
        }

        public Stream StartNewStream()
        {
            if (_reading)
                throw new NotSupportedException("The temp file was already moved to reading mode");

            _previousInstance?.Flush();
            _previousInstance = _environment.Options.Encryption.IsEnabled
                ? new InnerStream(new TempCryptoStream(_file), this)
                : new InnerStream(new InnerPartStream(_file), this);

            return _previousInstance;
        }

        public long Generation;

        public IDisposable Scope()
        {
            return new DisposableAction(() =>
            {
                _previousInstance = null;

                Generation++;
                _reading = false;
                _file.ResetLength();

                const int _128mb = 128 * 1024 * 1024;
                if (_file.InnerStream.Length > _128mb)
                    _file.InnerStream.SetLength(_128mb);
            });
        }

        public void Dispose()
        {
            try
            {
                _file.Dispose();
            }
            finally
            {
                PosixFile.DeleteOnClose(_tempFile);
            }
        }

        private class InnerStream : Stream
        {
            private readonly Stream _stream;
            private readonly StreamsTempFile _parent;

            private readonly long _generation;

            public InnerStream(Stream stream, StreamsTempFile parent)
            {
                _stream = stream ?? throw new ArgumentNullException(nameof(stream));
                _parent = parent ?? throw new ArgumentNullException(nameof(parent));
                _generation = parent.Generation;
            }

            public override void Flush()
            {
                if (CanWrite == false)
                    throw new NotSupportedException();

                _stream.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (CanRead == false)
                    throw new NotSupportedException();

                _parent._reading = true;

                return _stream.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                AssertGeneration();

                _parent._reading = true;

                return _stream.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                AssertGeneration();

                _stream.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (CanWrite == false)
                    throw new NotSupportedException();

                _stream.Write(buffer, offset, count);
            }

            public override bool CanRead => _parent.Generation == _generation;

            public override bool CanSeek => true;

            public override bool CanWrite => _parent._reading == false && _parent.Generation == _generation;

            public override long Length => _stream.Length;

            public override long Position
            {
                get => _stream.Position;
                set
                {
                    AssertGeneration();

                    _stream.Position = value;
                }
            }

            protected override void Dispose(bool disposing)
            {
                AssertGeneration();

                _stream.Dispose();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AssertGeneration()
            {
                if (_parent.Generation != _generation)
                    throw new NotSupportedException($"Invalid generation. Parent: {_parent.Generation}. Current: {_generation}");
            }
        }

        private class InnerPartStream : Stream
        {
            private readonly Stream _file;
            private readonly long _startPosition;
            private long _length;

            public InnerPartStream(Stream file)
            {
                _file = file;
                _startPosition = file.Position;
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                var remaining = (_startPosition + _length) - _file.Position;
                if (remaining < count)
                    count = (int)remaining;
                return _file.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                if (origin != SeekOrigin.Begin || offset != 0)
                    throw new NotSupportedException();

                return _file.Seek(_startPosition + offset, origin);
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                _file.Write(buffer, offset, count);
                _length += count;
            }

            public override bool CanRead => true;

            public override bool CanSeek => true;

            public override bool CanWrite => true;

            public override long Length => _length;

            public override long Position
            {
                get => throw new NotSupportedException();
                set => Seek(value, SeekOrigin.Begin);
            }
        }
    }
}
