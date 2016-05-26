using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Sparrow.Collections;

namespace Raven.Server.Json
{
    public class WebsocketStream : Stream
    {
        private readonly WebSocket _webSocket;
        private readonly CancellationToken _cancellationToken;
        private readonly byte[] _buffer;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;

        private readonly ConcurrentSet<Task> _activeWriteTasks = new ConcurrentSet<Task>();

        private volatile bool _isDisposed;

        /// <summary>
        /// Initialize the stream. Assumes the websocket is initialized and connected
        /// </summary>
        /// <remarks>This is not a thread-safe implementation</remarks>
        /// <param name="webSocket"></param>
        public WebsocketStream(WebSocket webSocket, CancellationToken cancellationToken, byte[] buffer = null)
        {
            _isDisposed = false;
            if (webSocket == null)
                throw new ArgumentNullException(nameof(webSocket));
            if (webSocket.State != WebSocketState.Open)
                throw new InvalidOperationException("The passed websocket is not open, it must be open when passed to the WebSocketStream");
            _webSocket = webSocket;
            _cancellationToken = cancellationToken;
            _buffer = buffer ?? new byte[1024];
        }

        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

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

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            ThrowOnDisposed();

            await _webSocket.SendAsync(new ArraySegment<byte>(buffer),
                WebSocketMessageType.Text,
                false, cancellationToken).ConfigureAwait(false);
        }

        private static readonly ArraySegment<byte> emptyBuffer = new ArraySegment<byte>(new byte[0]);
        public async Task WriteEndOfMessageAsync()
        {
            ThrowOnDisposed();

            await _webSocket.SendAsync(emptyBuffer,
                WebSocketMessageType.Text,
                true, _cancellationToken).ConfigureAwait(false);
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
            ThrowOnDisposed();

            int read = 0;
            while (read < count)
            {
                var bufferSegment = new ArraySegment<byte>(buffer, read, count - read);
                var result = AsyncHelpers.RunSync(() => 
                                _webSocket.ReceiveAsync(bufferSegment, _cancellationToken));

                read += result.Count;
                if (result.EndOfMessage)
                    break;
            }

            return read;
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

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            ThrowOnDisposed();
            
            int read = 0;
            while (read < count)
            {
                var bufferSegment = new ArraySegment<byte>(buffer, read, count - read);
                var result = await _webSocket.ReceiveAsync(bufferSegment, cancellationToken).ConfigureAwait(false);
                read += result.Count;

                if (result.EndOfMessage)
                    break;
            }

            return read;
        }

        private void ThrowOnDisposed()
        {
            if(_isDisposed)
                throw new ObjectDisposedException("Cannot use WebsocketStream after it was disposed");
        }

        public override void Flush()
        {
            ThrowOnDisposed();
            if(_activeWriteTasks.Count > 0)
                AsyncHelpers.RunSync(() =>
                    Task.WhenAll(_activeWriteTasks)
                        .ContinueWith(t => _activeWriteTasks.Clear(), _cancellationToken));
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            if(_activeWriteTasks.Count > 0)
                await Task.WhenAll(_activeWriteTasks)
                          .ContinueWith(t => 
                                _activeWriteTasks.Clear(), cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            AsyncHelpers.RunSync(() => 
                Task.WhenAll(_activeWriteTasks)
                    .ContinueWith(t => _isDisposed = true, _cancellationToken));

        }
    }
}
