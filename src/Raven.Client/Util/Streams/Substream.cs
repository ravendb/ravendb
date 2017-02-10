using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.NewClient.Abstractions.Util.Streams
{
    public class Substream : Stream
    {
        private readonly bool ownStream;
        private Stream stream;

        private readonly long length;
        private readonly long start;

        public Substream(Stream stream, long start, long length, bool ownStream = false)
        {
            if ( stream == null)
                throw new ArgumentNullException("stream");

            this.stream = stream;
            this.start = start;
            this.length = length;
            this.ownStream = ownStream;
        }        

        public override bool CanRead
        {
            get { return stream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return stream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
            stream.Flush();
        }

        public override long Length
        {
            get { return length; }
        }

        public override long Position
        {
            get
            {
                long basePosition = stream.Position;
                if (basePosition - start < 0 || basePosition - start > length)
                    throw new ArgumentOutOfRangeException("value", "The base stream position is outside the current sub-stream access.");

                return basePosition - start;
            }
            set
            {
                if (value > length)
                    throw new ArgumentOutOfRangeException("value", "The position is outside the sub-stream access range.");

                stream.Position = start + value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            long n = length - Position;
            if (n > count) 
                n = count;
            if (n <= 0)
                return 0;

            return stream.Read(buffer, offset, (int)n);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (offset > Length)
                throw new ArgumentOutOfRangeException("offset", "The position is outside the sub-stream access range.");

            switch (origin)
            {
                case SeekOrigin.Begin:
                    return stream.Seek(start + offset, origin);
                case SeekOrigin.Current:
                    if ( Position + offset > Length)
                        throw new ArgumentOutOfRangeException("offset", "The position is outside the sub-stream access range.");

                    return stream.Seek(offset, SeekOrigin.Current);
                case SeekOrigin.End:
                    long endOffset = length - offset;
                    return stream.Seek(start + endOffset, SeekOrigin.Begin);
                default:
                    throw new ArgumentException("Invalid seek origin.", "origin");
            }
        }

        public override void SetLength(long value)
        {
            throw new InvalidOperationException("Substreams cannot change size. Create a new one instead.");
        }
        

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new InvalidOperationException("Not implemented");
        }


        // The bulk of the clean-up code is implemented in Dispose(bool)
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                // free managed resources
                if (ownStream && stream != null)
                {
                    stream.Dispose();
                    stream = null;
                }
            }
        }
    }
}
