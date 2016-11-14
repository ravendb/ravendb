// -----------------------------------------------------------------------
//  <copyright file="BufferPoolMemoryStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using Sparrow;

namespace Raven.NewClient.Abstractions.Util.Streams
{
    public class BufferPoolMemoryStream : Stream
    {       
        protected byte[] _buffer;
        protected ObjectPool<byte[]> _bufferPool;
        
        [CLSCompliant(false)]
        protected long _length;

        private int _position;

        public BufferPoolMemoryStream()
        {
            _bufferPool = BufferSharedPools.MicroByteArray;
            _buffer = _bufferPool.Allocate();          
        }

        protected override void Dispose(bool disposing)
        {
            // We will release the currently allocated buffer.
            if (_bufferPool != null)
                _bufferPool.Free(_buffer);

            _buffer = null;

            base.Dispose(disposing);
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    break;
                case SeekOrigin.Current:
                    Position += offset;
                    break;
                case SeekOrigin.End:
                    Position = Length - offset;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("origin");
            }

            return Position;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = (int)Math.Min(count, _length - _position);			

            Buffer.BlockCopy(_buffer, _position, buffer, offset, read);
            _position += read;
            return read;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            EnsureCapacity(_position + count);
            Debug.Assert(count <= _buffer.Length - _position, " EnsureCapacity() should grow the underlying buffer to a proper size");

            Buffer.BlockCopy(buffer, offset, _buffer, _position, count);

            _position += count;
            _length = Math.Max(_length, _position);
        }

        public override void SetLength(long value)
        {
            EnsureCapacity(value);
            _length = value;
        }

        private void EnsureCapacity(long requestedCapacity)
        {
            if (requestedCapacity <= _buffer.Length)
                return;

            //estimate that the needed buffer growth is at most twice the old length
            var estimatedNewCapacity = _buffer.Length * 2;

            //precaution -> to make sure casting long to int is ok (I doubt this will ever be not ok, but still)
            Debug.Assert(requestedCapacity <= Int32.MaxValue,"should never grow buffer to these sizes");


            //if the required capacity is more than the estimated growth, grow the buffer by the requested capacity
            var newCapacity = (requestedCapacity <= estimatedNewCapacity) ? estimatedNewCapacity : (int)requestedCapacity;
                        
            // We reset the buffer pool
            ObjectPool<byte[]> newBufferPool = null;

            // We will ensure to cap out to fit the capacity as best as we can.
            if (newCapacity <= BufferSharedPools.MicroByteBufferSize)
            {
                newCapacity = BufferSharedPools.MicroByteBufferSize;
                newBufferPool = BufferSharedPools.MicroByteArray;
            }
            else if (newCapacity <= BufferSharedPools.SmallByteBufferSize)
            {
                newCapacity = BufferSharedPools.SmallByteBufferSize;
                newBufferPool = BufferSharedPools.SmallByteArray;
            }
            else if (newCapacity <= BufferSharedPools.ByteBufferSize)
            {
                newCapacity = BufferSharedPools.ByteBufferSize;
                newBufferPool = BufferSharedPools.ByteArray;
            }
            else if (newCapacity <= BufferSharedPools.BigByteBufferSize)
            {
                newCapacity = BufferSharedPools.BigByteBufferSize;
                newBufferPool = BufferSharedPools.BigByteArray;

            }                    
            else if ( newCapacity <= BufferSharedPools.HugeByteBufferSize)
            {
                newCapacity = BufferSharedPools.HugeByteBufferSize;
                newBufferPool = BufferSharedPools.HugeByteArray;
            }

            byte[] newBuffer = (newBufferPool == null) ? new byte[newCapacity] : newBufferPool.Allocate();

            Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _position);

            // We will release the currently allocated buffer.
            if ( _bufferPool != null )
                _bufferPool.Free(_buffer);

            _buffer = newBuffer;
            _bufferPool = newBufferPool;
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length { get { return _length; } }

        public override long Position
        {
            get { return _position; }
            set
            {
                if (value < 0 || value >= _length)
                    throw new ArgumentOutOfRangeException("value", "Cannot set position to lower than 0 or higher than the length");
                _position = (int)value;
            }
        }
    }
}
