// -----------------------------------------------------------------------
//  <copyright file="PositionWrapperStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

namespace Raven.Abstractions.Util
{
    public class PositionWrapperStream : Stream
    {
        private readonly Stream wrapped;
        private readonly bool leaveOpen;

        private int pos = 0;

        public PositionWrapperStream(Stream wrapped, bool leaveOpen)
        {
            this.wrapped = wrapped;
            this.leaveOpen = leaveOpen;
        }

        public override bool CanSeek { get { return false; } }

        public override bool CanWrite { get { return true; } }

        public override long Position
        {
            get { return pos; }
            set { throw new NotSupportedException(); }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            pos += count;
            wrapped.Write(buffer, offset, count);
        }

        public override void Flush()
        {
            wrapped.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            if (leaveOpen == false)
                wrapped.Dispose();

            base.Dispose(disposing);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead
        {
            get { throw new NotSupportedException(); }
        }
        public override long Length
        {
            get { throw new NotSupportedException(); }
        }
    }
}
