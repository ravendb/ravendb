using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Client.Util
{
    /// <summary>
    /// Fails a read or a write that doesn't complete within the timeout of the stream.
    /// </summary>
    /// <remarks>
    /// PERF: an operation that completes synchronously (most of them, the transport buffers them) costs no timer and no allocation.
    /// Without a token of the caller, the operation gets a <see cref="CoarseTimeout"/> token. With one, the operation gets the
    /// caller's token and only an operation that has to wait registers to a <see cref="CoarseTimeout"/> token, which, when it fires,
    /// aborts the transport (when we know how, the server aborts the connection) or cancels a token linked to the caller's one.
    /// Either way the operation completes before we do, so its buffer is never in use after it failed.
    /// </remarks>
    internal class StreamWithTimeout : Stream, IAsyncDisposable
    {
        private static readonly TimeSpan DefaultWriteTimeout = TimeSpan.FromSeconds(120);
        internal static TimeSpan DefaultReadTimeout { get; } = TimeSpan.FromSeconds(120);

        internal readonly Stream _stream;
        private readonly Action<object> _abort;
        private readonly object _abortState;
        private int _writeTimeout = (int)DefaultWriteTimeout.TotalMilliseconds;
        private int _readTimeout = (int)DefaultReadTimeout.TotalMilliseconds;

        // linked to the token of the caller, so a timeout can cancel an operation when we can't abort the transport
        private CancellationTokenSource _linked;
        private bool _timedOut;

#if DEBUG
        private CancellationToken _callerToken;
#endif

        private long _totalRead = 0;
        private long _totalWritten = 0;

        /// <param name="abort">Aborts the transport under the stream, so an operation that waits fails when it times out.</param>
        public StreamWithTimeout(Stream stream, Action<object> abort = null, object abortState = null)
        {
            _stream = stream;
            _abort = abort;
            _abortState = abortState;
        }

        public override int ReadTimeout
        {
            get => _readTimeout;
            set => _readTimeout = value;
        }

        public override int WriteTimeout
        {
            get => _writeTimeout;
            set => _writeTimeout = value;
        }

        public override bool CanTimeout => true;

        public override void Flush()
        {
            _stream.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return _stream.FlushAsync(cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            // _totalRead is counted in ReadAsyncWithTimeout
            return AsyncHelpers.RunSync(() => ReadAsyncWithTimeout(buffer, offset, count, CancellationToken.None));
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
#if NETSTANDARD2_0
            // _totalRead is counted in ReadAsyncWithTimeout
            return ReadAsyncWithTimeout(buffer, offset, count, cancellationToken);
#else
            return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
#endif
        }

#if !NETSTANDARD2_0
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.CanBeCanceled == false)
            {
                var timeout = CoarseTimeout.After(_readTimeout);
                var readWithTimeout = _stream.ReadAsync(buffer, timeout.Token);
                if (timeout.IsShared && readWithTimeout.IsCompletedSuccessfully)
                    return new ValueTask<int>(CountRead(readWithTimeout.Result));

                return CompleteReadAsync(readWithTimeout, timeout);
            }

            var readTask = _stream.ReadAsync(buffer, GetOperationToken(cancellationToken));
            if (readTask.IsCompletedSuccessfully)
                return new ValueTask<int>(CountRead(readTask.Result));

            return WaitForReadAsync(readTask.AsTask());
        }

        private async ValueTask<int> CompleteReadAsync(ValueTask<int> readTask, CoarseTimeout.TimeoutToken timeout)
        {
            using (timeout)
                return CountRead(await readTask.ConfigureAwait(false));
        }
#endif

        private async Task<int> ReadAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled == false)
            {
                using (var timeout = CoarseTimeout.After(_readTimeout))
                    return CountRead(await _stream.ReadAsync(buffer, offset, count, timeout.Token).ConfigureAwait(false));
            }

            var readTask = _stream.ReadAsync(buffer, offset, count, GetOperationToken(cancellationToken));
            if (readTask.IsCompleted)
                return CountRead(await readTask.ConfigureAwait(false));

            return await WaitForReadAsync(readTask).ConfigureAwait(false);
        }

        private async ValueTask<int> WaitForReadAsync(Task<int> readTask)
        {
            await WaitWithTimeoutAsync(readTask, _readTimeout, "read").ConfigureAwait(false);
            return CountRead(await readTask.ConfigureAwait(false));
        }

        private int CountRead(int read)
        {
            _totalRead += read;
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _stream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _totalWritten += count;

            AsyncHelpers.RunSync(() => WriteAsyncWithTimeout(buffer, offset, count, CancellationToken.None));
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _totalWritten += count;

            return WriteAsyncWithTimeout(buffer, offset, count, cancellationToken);
        }

        private Task WriteAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled == false)
            {
                var timeout = CoarseTimeout.After(_writeTimeout);
                var writeWithTimeout = _stream.WriteAsync(buffer, offset, count, timeout.Token);
                if (timeout.IsShared)
                    return writeWithTimeout;

                return CompleteWriteAsync(writeWithTimeout, timeout);
            }

            var writeTask = _stream.WriteAsync(buffer, offset, count, GetOperationToken(cancellationToken));
            if (writeTask.IsCompleted)
                return writeTask;

            return WaitWithTimeoutAsync(writeTask, _writeTimeout, "write");
        }

        private static async Task CompleteWriteAsync(Task writeTask, CoarseTimeout.TimeoutToken timeout)
        {
            using (timeout)
                await writeTask.ConfigureAwait(false);
        }

        private CancellationToken GetOperationToken(CancellationToken cancellationToken)
        {
            // PERF: when we can abort the transport, the operation gets the caller's token as is, no linking
            if (_abort != null)
                return cancellationToken;

            var linked = Volatile.Read(ref _linked);
            if (linked == null)
            {
                linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var existing = Interlocked.CompareExchange(ref _linked, linked, null);
                if (existing != null)
                {
                    linked.Dispose();
                    linked = existing;
                }
#if DEBUG
                else
                {
                    _callerToken = cancellationToken;
                }
#endif
            }
#if DEBUG
            else if (_callerToken != cancellationToken)
            {
                throw new InvalidOperationException("The cancellation token was changed during the request");
            }
#endif

            return linked.Token;
        }

        private async Task WaitWithTimeoutAsync(Task operation, int timeoutInMs, string operationName)
        {
            using (var timeout = CoarseTimeout.After(timeoutInMs))
            using (timeout.Token.Register(static state => ((StreamWithTimeout)state).OnTimeout(), this))
            {
                try
                {
                    await operation.ConfigureAwait(false);
                }
                catch (Exception e) when (Volatile.Read(ref _timedOut) && e is not OperationCanceledException)
                {
                    // aborting the transport fails the operation with an I/O error, a timeout is reported as a cancellation
                    throw new OperationCanceledException($"The {operationName} didn't complete within {timeoutInMs} ms", e);
                }
            }
        }

        private void OnTimeout()
        {
            Volatile.Write(ref _timedOut, true);

            // not on the timer's thread, aborting can run the continuations of the operation
            ThreadPool.QueueUserWorkItem(static state => ((StreamWithTimeout)state).Abort(), this);
        }

        private void Abort()
        {
            try
            {
                if (_abort != null)
                    _abort(_abortState);
                else
                    Volatile.Read(ref _linked)?.Cancel();
            }
            catch
            {
                // the operation fails (or completes) on its own
            }
        }

        public override bool CanRead => _stream.CanRead;
        public override bool CanSeek => _stream.CanSeek;
        public override bool CanWrite => _stream.CanWrite;
        public override long Length => _stream.Length;

        public long TotalRead => _totalRead;
        public long TotalWritten => _totalWritten;

        public override long Position
        {
            get => _stream.Position;
            set => _stream.Position = value;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            _stream.Dispose();

            _linked?.Dispose();
        }

#if NETSTANDARD2_0
        public ValueTask DisposeAsync()
#else
        public override async ValueTask DisposeAsync()
#endif
        {
#if NETSTANDARD2_0
            Dispose();
            return new ValueTask();
#else
            await base.DisposeAsync().ConfigureAwait(false);
            await _stream.DisposeAsync().ConfigureAwait(false);

            _linked?.Dispose();
#endif
        }
    }
}
