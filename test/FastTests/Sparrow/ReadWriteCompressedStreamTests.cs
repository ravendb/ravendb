using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class ReadWriteCompressedStreamTests : NoDisposalNeeded
    {
        public ReadWriteCompressedStreamTests(ITestOutputHelper output) : base(output)
        {
        }

        /// <summary>
        /// <see cref="ReadWriteCompressedStream"/> multiplexes an independent read side and write side over a single
        /// inner stream, and in production that inner stream is an <see cref="System.Net.Security.SslStream"/>, which
        /// rejects a write issued while another write is still pending. Disposal therefore may not write to the inner
        /// stream at all: on a live replication connection the sender can be parked inside a write (a full socket send
        /// buffer toward a dying node) while the server shutdown token fires a cancellation callback that disposes that
        /// same stream - JsonOperationContext.ParseToMemoryAsync registers stream.Dispose() to unblock a pending read.
        /// </summary>
        [RavenFact(RavenTestCategory.Core)]
        public async Task DisposeMustNotWriteToInnerStreamWhileAWriteIsPending()
        {
            var inner = new SocketLikeStream();
            var compressed = new ReadWriteCompressedStream(inner);

            var payload = Incompressible(1024 * 1024);
            var write = Task.Run(() => compressed.Write(payload, 0, payload.Length));

            Assert.True(inner.WriteEntered.Wait(TimeSpan.FromSeconds(30)), "The writer never reached the inner stream.");

            inner.RecordWritesFromNowOn();
            compressed.Dispose();

            await Assert.ThrowsAnyAsync<Exception>(() => write);

            Assert.Null(inner.ConcurrentWriteRejectedAt);
            Assert.Equal(0, inner.WritesAfterDisposalStarted);
            Assert.True(inner.Disposed, "The inner stream was not disposed, so the connection leaked.");
        }

        /// <summary>
        /// Disposal aborts the transport instead of waiting for a write that may never drain: disposing the inner
        /// stream fails the pending write straight away, so the writer unwinds and releases the compression stream's
        /// dispose lock. Without that, disposal would block for as long as the socket takes to give up on the send -
        /// inside a cancellation callback on the server shutdown path.
        /// </summary>
        [RavenFact(RavenTestCategory.Core)]
        public async Task DisposeAbortsAWritePendingOnTheInnerStream()
        {
            var inner = new SocketLikeStream();
            var compressed = new ReadWriteCompressedStream(inner);

            var payload = Incompressible(1024 * 1024);
            var write = Task.Run(() => compressed.Write(payload, 0, payload.Length));

            Assert.True(inner.WriteEntered.Wait(TimeSpan.FromSeconds(30)), "The writer never reached the inner stream.");

            // Nothing here ever releases the parked write - disposal has to break it loose on its own.
            var dispose = Task.Run(() => compressed.Dispose());

            Assert.True(await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(15))) == dispose,
                "Dispose() blocked on the pending write instead of aborting the inner stream.");
            await dispose;

            // The writer has to learn the connection is gone, and it has to learn it that way: a NullReferenceException
            // would mean the compression buffer was released from under it while it was still writing.
            var error = await Assert.ThrowsAnyAsync<Exception>(() => write);
            Assert.IsType<ObjectDisposedException>(error);
        }

        private static byte[] Incompressible(int size)
        {
            // Random bytes, and more than one zstd block worth of them, so that compression actually produces output
            // and reaches the inner stream instead of being buffered inside zstd.
            var buffer = new byte[size];
            new Random(1337).NextBytes(buffer);
            return buffer;
        }

        /// <summary>
        /// Models the two traits of the <see cref="System.Net.Security.SslStream"/> over a socket that this test class
        /// depends on: a write issued while another write is pending is rejected rather than serialized, and disposing
        /// the stream fails a pending write instead of leaving it parked. The first write parks until the stream is
        /// disposed, which is the window a concurrent disposal has to hit.
        /// </summary>
        private sealed class SocketLikeStream : Stream
        {
            public readonly ManualResetEventSlim WriteEntered = new ManualResetEventSlim(false);

            public string ConcurrentWriteRejectedAt;
            public int WritesAfterDisposalStarted;
            public bool Disposed;

            private readonly ManualResetEventSlim _writeParked = new ManualResetEventSlim(false);
            private int _writePending;
            private bool _parked;
            private bool _recordWrites;

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => throw new NotSupportedException();

            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            /// <summary>
            /// Starts counting writes, so that a test can tell writes issued by disposal apart from the ones the parked
            /// writer had already made on its way in.
            /// </summary>
            public void RecordWritesFromNowOn() => _recordWrites = true;

            public override void Write(byte[] buffer, int offset, int count) => WriteCore();

            public override void Write(ReadOnlySpan<byte> buffer) => WriteCore();

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                WriteCore();
                return Task.CompletedTask;
            }

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                WriteCore();
                return ValueTask.CompletedTask;
            }

            private void WriteCore()
            {
                if (_recordWrites)
                    Interlocked.Increment(ref WritesAfterDisposalStarted);

                if (Disposed)
                    throw new ObjectDisposedException(nameof(SocketLikeStream));

                if (Interlocked.CompareExchange(ref _writePending, 1, 0) != 0)
                {
                    ConcurrentWriteRejectedAt = Environment.StackTrace;
                    throw new NotSupportedException(" This method may not be called when another write operation is pending.");
                }

                try
                {
                    if (_parked)
                        return;

                    _parked = true;
                    WriteEntered.Set();
                    _writeParked.Wait();

                    throw new ObjectDisposedException(nameof(SocketLikeStream));
                }
                finally
                {
                    Interlocked.Exchange(ref _writePending, 0);
                }
            }

            public override void Flush()
            {
            }

            public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public override int Read(byte[] buffer, int offset, int count) => 0;

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            protected override void Dispose(bool disposing)
            {
                Disposed = true;

                // A real socket fails whatever send was in flight when it is closed, rather than holding the caller.
                _writeParked.Set();

                base.Dispose(disposing);
            }
        }
    }
}
