using System;
using System.Diagnostics;
using System.IO;
using Sparrow.Binary;

namespace Raven.Server.Rachis.Remote
{
    public class RemoteReaderToStream : RemoteReader
    {
        private readonly Stream _stream;

        public RemoteReaderToStream(RemoteConnection parent, Stream stream) 
            : base(parent)
        {
            _stream = stream;
        }

        protected override int Read(int size)
        {
            var read = base.Read(size);
            _stream.Write(Buffer, 0, read);
            return read;
        }
    }
        
    public class StreamReader : Reader
    {
        private readonly Stream _stream;

        public StreamReader(Stream stream)
        {
            _stream = stream;
        }

        protected override int InternalRead(int offset, int count) => _stream.Read(Buffer, offset, count);
    }
        
    public class RemoteReader : Reader
    {
        private readonly RemoteConnection _parent;

        public RemoteReader(RemoteConnection parent)
        {
            _parent = parent;
        }

        protected override int InternalRead(int offset, int count) => _parent.Read(Buffer, offset, count);
    }
    
    public abstract class Reader
    {
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

        public byte[] Buffer
        {
            get;
            private set;
        } = new byte[1024];

        public void ReadExactly(int size)
        {
            var read = Read(size);
            if(read < size)
                throw new EndOfStreamException();
        }

        protected virtual int Read(int size)
        {
            if (Buffer.Length < size)
                Buffer = new byte[Bits.PowerOf2(size)];
            var totalRead = 0;
            while (totalRead < size)
            {
                var read = InternalRead(totalRead, size - totalRead);
                if (read == 0)
                    break;
                totalRead += read;
            }

            Debug.Assert(totalRead <= size);
            return totalRead;
        }
            
        protected abstract int InternalRead(int offset, int count);
    }
}
