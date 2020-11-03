using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Server.Utils
{
    /// <summary>
    /// https://stackoverflow.com/questions/1475747/is-there-an-in-memory-stream-that-blocks-like-a-file-stream
    /// </summary>
    internal class EchoStream : Stream
    {
        public override bool CanTimeout { get; } = true;
        public override int ReadTimeout { get; set; } = Timeout.Infinite;
        public override int WriteTimeout { get; set; } = Timeout.Infinite;
        public override bool CanRead { get; } = true;
        public override bool CanSeek { get; }
        public override bool CanWrite { get; } = true;

        public bool CopyBufferOnWrite { get; set; }

        private readonly object _lock = new object();

        // Default underlying mechanism for BlockingCollection is ConcurrentQueue<T>, which is what we want
        private readonly BlockingCollection<byte[]> _buffers;

        private int _maxQueueDepth = 10;

        private byte[] m_buffer;
        private int m_offset;
        private int m_count;

        private bool m_Closed;

        public override void Close()
        {
            m_Closed = true;

            // release any waiting writes
            _buffers.CompleteAdding();
        }

        public bool DataAvailable
        {
            get
            {
                return _buffers.Count > 0;
            }
        }

        private long _length;

        public override long Length
        {
            get
            {
                return _length;
            }
        }

        private long _position;
        private Task _task;

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public EchoStream() : this(10)
        {
        }

        public EchoStream(int maxQueueDepth)
        {
            _maxQueueDepth = maxQueueDepth;
            _buffers = new BlockingCollection<byte[]>(_maxQueueDepth);
        }

        // we override the xxxxAsync functions because the default base class shares state between ReadAsync and WriteAsync, which causes a hang if both are called at once
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return Task.Run(() => Write(buffer, offset, count));
        }

        // we override the xxxxAsync functions because the default base class shares state between ReadAsync and WriteAsync, which causes a hang if both are called at once
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return Task.Run(() => Read(buffer, offset, count));
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (m_Closed || buffer.Length - offset < count || count <= 0)
                return;

            AssertTask();

            byte[] newBuffer;
            if (!CopyBufferOnWrite && offset == 0 && count == buffer.Length)
                newBuffer = buffer;
            else
            {
                newBuffer = new byte[count];
                Buffer.BlockCopy(buffer, offset, newBuffer, 0, count);
            }
            if (!_buffers.TryAdd(newBuffer, WriteTimeout))
                throw new TimeoutException("EchoStream Write() Timeout");

            _length += count;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count == 0)
                return 0;
            lock (_lock)
            {
                AssertTask();

                if (m_count == 0 && _buffers.Count == 0)
                {
                    if (m_Closed)
                        return -1;

                    if (_buffers.TryTake(out m_buffer, ReadTimeout))
                    {
                        m_offset = 0;
                        m_count = m_buffer.Length;
                    }
                    else
                    {
                        AssertTask();
                        return m_Closed ? -1 : 0;
                    }
                }

                int returnBytes = 0;
                while (count > 0)
                {
                    if (m_count == 0)
                    {
                        if (_buffers.TryTake(out m_buffer, 0))
                        {
                            m_offset = 0;
                            m_count = m_buffer.Length;
                        }
                        else
                        {
                            AssertTask();
                            break;
                        }
                    }

                    var bytesToCopy = (count < m_count) ? count : m_count;
                    Buffer.BlockCopy(m_buffer, m_offset, buffer, offset, bytesToCopy);
                    m_offset += bytesToCopy;
                    m_count -= bytesToCopy;
                    offset += bytesToCopy;
                    count -= bytesToCopy;

                    returnBytes += bytesToCopy;
                }

                _position += returnBytes;

                return returnBytes;
            }
        }

        internal void TaskToWatch(Task task)
        {
            if (task == null)
                throw new ArgumentNullException(nameof(task));

            _task = task.ContinueWith(_ => _buffers.CompleteAdding());
        }

        public override int ReadByte()
        {
            byte[] returnValue = new byte[1];
            return Read(returnValue, 0, 1) <= 0 ? -1 : returnValue[0];
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AssertTask()
        {
            var task = _task;
            if (task == null)
                return;

            if (task.IsFaulted)
                task.Wait();
        }
    }
}
