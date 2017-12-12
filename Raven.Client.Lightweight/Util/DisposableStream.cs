// -----------------------------------------------------------------------
//  <copyright file="DisposableStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
#if !DNXCORE50
using System.Runtime.Remoting;
#endif
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    public class DisposableStream : Stream
    {
        private readonly Stream innerStream;

        private readonly Action onDispose;

        public DisposableStream(Stream stream, Action onDispose)
        {
            innerStream = stream;
            this.onDispose = onDispose;
        }

        public override bool CanRead
        {
            get
            {
                return innerStream.CanRead;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return innerStream.CanSeek;
            }
        }

        public override bool CanTimeout
        {
            get
            {
                return innerStream.CanTimeout;
            }
        }
        public override bool CanWrite
        {
            get
            {
                return innerStream.CanWrite;
            }
        }

        public override long Length
        {
            get
            {
                return innerStream.Length;
            }
        }

        public override long Position
        {
            get
            {
                return innerStream.Position;
            }
            set
            {
                innerStream.Position = value;
            }
        }

        public override int ReadTimeout
        {
            get
            {
                return innerStream.ReadTimeout;
            }
            set
            {
                innerStream.ReadTimeout = value;
            }
        }

        public override int WriteTimeout
        {
            get
            {
                return innerStream.WriteTimeout;
            }
            set
            {
                innerStream.WriteTimeout = value;
            }
        }

#if !DNXCORE50
        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            return innerStream.BeginRead(buffer, offset, count, callback, state);
        }

        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            return innerStream.BeginWrite(buffer, offset, count, callback, state);
        }

        public override void Close()
        {
            innerStream.Close();
            base.Close();
        }
#endif

        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            return innerStream.CopyToAsync(destination, bufferSize, cancellationToken);
        }

#if !DNXCORE50
        public override ObjRef CreateObjRef(Type requestedType)
        {
            return innerStream.CreateObjRef(requestedType);
        }

        public override int EndRead(IAsyncResult asyncResult)
        {
            return innerStream.EndRead(asyncResult);
        }

        public override void EndWrite(IAsyncResult asyncResult)
        {
            innerStream.EndWrite(asyncResult);
        }
#endif

        public override bool Equals(object obj)
        {
            return innerStream.Equals(obj);
        }

        public override void Flush()
        {
            innerStream.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return innerStream.FlushAsync(cancellationToken);
        }

        public override int GetHashCode()
        {
            return innerStream.GetHashCode();
        }

#if !DNXCORE50
        public override object InitializeLifetimeService()
        {
            return innerStream.InitializeLifetimeService();
        }
#endif

        public override int Read(byte[] buffer, int offset, int count)
        {
            return innerStream.Read(buffer, offset, count);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return innerStream.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override int ReadByte()
        {
            return innerStream.ReadByte();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return innerStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            innerStream.SetLength(value);
        }

        public override string ToString()
        {
            return innerStream.ToString();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            innerStream.Write(buffer, offset, count);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return innerStream.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override void WriteByte(byte value)
        {
            innerStream.WriteByte(value);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                innerStream.Dispose();
                base.Dispose(disposing);
            }
            finally
            {
                if (onDispose != null)
                    onDispose();
            }
        }
    }
}
