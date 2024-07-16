using System;
using System.Buffers;
using System.IO;
using Sparrow;
using Sparrow.Binary;

namespace Raven.Server.Rachis.Remote
{
    public class RemoteToStreamSnapshotReader : RemoteSnapshotReader
    {
        private readonly Stream _stream;

        public RemoteToStreamSnapshotReader(RachisLogRecorder logger, RemoteConnection parent, Stream stream) 
            : base(logger, parent)
        {
            _stream = stream;
        }

        public override void ReadExactly(int size)
        {
            base.ReadExactly(size);
            _stream.Write(Buffer, 0, size);
        }
    }
        
    public class StreamSnapshotReader : SnapshotReader
    {
        private readonly Stream _stream;

        public StreamSnapshotReader(RachisLogRecorder logger, Stream stream) : base(logger)
        {
            _stream = stream;
        }

        protected override int InternalRead(int offset, int count) => _stream.Read(Buffer, offset, count);
    }
        
    public class RemoteSnapshotReader : SnapshotReader
    {
        private readonly RemoteConnection _parent;

        public RemoteSnapshotReader(RachisLogRecorder logger, RemoteConnection parent) : base(logger)
        {
            _parent = parent;
        }

        protected override int InternalRead(int offset, int count) => _parent.Read(Buffer, offset, count);
    }
    
    public abstract class SnapshotReader : IDisposable
    {
        private readonly RachisLogRecorder _logger;
        public byte[] Buffer { get; private set;}

        private uint _readAttempts;
        private long _totalBytes;
        protected SnapshotReader(RachisLogRecorder logger)
        {
            _logger = logger;
            Buffer = ArrayPool<byte>.Shared.Rent(1024);
        }
        
        public int ReadInt32()
        {
            ReadExactly(sizeof(int));
            return BitConverter.ToInt32(Buffer, 0);
        }

        public long ReadInt64()
        {
            ReadExactly(sizeof(long));
            return BitConverter.ToInt64(Buffer, 0);
        }

        public virtual void ReadExactly(int size)
        {
            _totalBytes += size;
            if (++_readAttempts % 256 == 0)
            {
                _logger.Record($"Read snapshot total size {new Size(_totalBytes, SizeUnit.Bytes)}");
            }

            if (Buffer.Length < size)
            {
                ArrayPool<byte>.Shared.Return(Buffer);
                Buffer = ArrayPool<byte>.Shared.Rent(Bits.PowerOf2(size));
            }
            var totalRead = 0;
            while (totalRead < size)
            {
                var read = InternalRead(totalRead, size - totalRead);
                if (read == 0)
                    throw new EndOfStreamException();
                totalRead += read;
            }
        }
            
        protected abstract int InternalRead(int offset, int count);
        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(Buffer);
        }
    }
}
