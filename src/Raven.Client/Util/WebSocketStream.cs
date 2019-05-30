using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections;
using Sparrow.Threading;

namespace Raven.Client.Util
{
    internal class WebSocketStream : Stream
    {
        private readonly WebSocket _webSocket;
        private readonly CancellationToken _cancellationToken;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;

        private readonly ConcurrentSet<Task> _activeWriteTasks = new ConcurrentSet<Task>();

        private SingleUseFlag _isDisposed = new SingleUseFlag();

        /// <summary>
        /// Initialize the stream. Assumes the websocket is initialized and connected
        /// </summary>
        /// <remarks>This is not a thread-safe implementation</remarks>
        public WebSocketStream(WebSocket webSocket, CancellationToken cancellationToken)
        {
            if (webSocket == null)
                throw new ArgumentNullException(nameof(webSocket));
            if (webSocket.State != WebSocketState.Open)
                throw new InvalidOperationException("The passed websocket is not open, it must be open when passed to the WebSocketStream");
            _webSocket = webSocket;
            _cancellationToken = cancellationToken;
        }

        public override long Length => throw new NotSupportedException();

        public override long Position { get; set; }

        public override void Write(byte[] buffer, int offset, int count)
        {
            ThrowOnDisposed();
            var sendTask = _webSocket.SendAsync(new ArraySegment<byte>(buffer), 
                                                WebSocketMessageType.Text, 
                                                false,_cancellationToken);
            _activeWriteTasks.Add(sendTask);
            sendTask.ContinueWith(t => _activeWriteTasks.TryRemove(t), _cancellationToken);
        }

        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is <see langword="null" />.</exception>
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            ThrowOnDisposed();

            await _webSocket.SendAsync(new ArraySegment<byte>(buffer),
                WebSocketMessageType.Text,
                false, cancellationToken).ConfigureAwait(false);
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("Makes no sense for a websocket stream");
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException("Makes no sense for a websocket stream");
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        //reading and writing byte-by-byte does not make sense 
        public override void WriteByte(byte value)
        {
            throw new NotSupportedException();
        }

        //reading and writing byte-by-byte does not make sense 
        public override int ReadByte()
        {
            throw new NotSupportedException();
        }

        /// <exception cref="ArgumentNullException"><paramref name="buffer" /> is null.</exception>
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            ThrowOnDisposed();
            
            int read = 0;
            while (read < count)
            {
                var bufferSegment = new ArraySegment<byte>(buffer, offset + read, count - read);
                var result = await _webSocket.ReceiveAsync(bufferSegment, cancellationToken).ConfigureAwait(false);
                read += result.Count;

                if (result.EndOfMessage)
                    break;
            }

            return read;
        }

        private void ThrowOnDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException("Cannot use WebsocketStream after it was disposed");
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            if (_activeWriteTasks.Count > 0)
            {
                await Task.WhenAll(_activeWriteTasks).ConfigureAwait(false);
                _activeWriteTasks.Clear();
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            AsyncHelpers.RunSync(() => Task.WhenAll(_activeWriteTasks));
            _isDisposed.Raise();
        }
    }
}
