﻿using System;
using System.IO;
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
        private readonly FileStream _file;
        internal bool _reading;
        private Stream _previousInstance;
        public StreamsTempFile(string tempFile, StorageEnvironment environment)
        {
            _tempFile = tempFile;
            _environment = environment;

            _file = SafeFileStream.Create(_tempFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose | FileOptions.SequentialScan);
        }

        public Stream StartNewStream()
        {
            if (_reading)
                throw new NotSupportedException("The temp file was already moved to reading mode");

            _previousInstance?.Flush();
            if (_environment.Options.Encryption.IsEnabled)
            {
                _previousInstance = new TempCryptoStream(_file, this);
            }
            else
            {
                _previousInstance = new InnerPartStream(_file, this);
            }
            return _previousInstance;
        }

        public void Reset(int maxSizeInBytes = 128 * 1024 * 1024)
        {
            _reading = false;

            if (_file.Length > maxSizeInBytes)
                _file.SetLength(maxSizeInBytes);
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

        private class InnerPartStream : Stream
        {
            private readonly Stream _file;
            private readonly StreamsTempFile _parent;
            private readonly long _startPosition;
            private long _length;
            private bool _reading;

            public InnerPartStream(Stream file, StreamsTempFile parent)
            {
                _file = file;
                _parent = parent;
                _startPosition = file.Position;
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (_reading == false)
                    throw new NotSupportedException();

                var remaining = (_startPosition + _length) - _file.Position;
                if (remaining < count)
                    count = (int)remaining;
                return _file.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                if (origin != SeekOrigin.Begin || offset != 0)
                    throw new NotSupportedException();

                _reading = true;
                _parent._reading = true;
                return _file.Seek(_startPosition + offset, origin);
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (_reading)
                    throw new NotSupportedException();
                _file.Write(buffer, offset, count);
                _length += count;
            }

            public override bool CanRead => _reading;

            public override bool CanSeek { get; } = true;

            public override bool CanWrite => _reading == false;

            public override long Length => _length;

            public override long Position
            {
                get => throw new NotSupportedException();
                set => Seek(value, SeekOrigin.Begin);
            }
        }
    }
}
