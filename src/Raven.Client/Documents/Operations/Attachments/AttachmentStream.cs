using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class AttachmentStream : Stream
    {
        private HttpResponseMessage _response;
        private Stream _stream;

        public AttachmentStream(HttpResponseMessage response, Stream stream)
        {
            _response = response;
            _stream = stream;
        }

        protected override void Dispose(bool disposing)
        {
            _stream?.Dispose();
            _stream = null;
            _response?.Dispose();
            _response = null;
        }

        public override void Flush()
        {
            ThrowNotWritableStream();
        }

        private void ThrowNotWritableStream()
        {
            throw new NotSupportedException("Attachment stream is not writable");
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _stream.Read(buffer, offset, count);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _stream.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            ThrowNotWritableStream();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            ThrowNotWritableStream();
        }

        public override bool CanRead { get; } = true;
        public override bool CanSeek => _stream.CanSeek;
        public override bool CanWrite { get; } = false;
        public override long Length => _stream.Length;

        public override long Position
        {
            get => _stream.Position;
            set => _stream.Position = value;
        }
    }
}
