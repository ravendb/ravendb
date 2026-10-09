using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Zstd.Benchmark.Infrastructure
{
    public enum StreamTarget
    {
        /// <summary>In-memory stream: measures compression plus the managed/native call overhead only.</summary>
        Memory,

        /// <summary>Unbuffered OS handle: every Read/Write on the inner stream is a system call, like sockets and files in production.</summary>
        Syscall
    }

    internal static class TestStreams
    {
        /// <summary>
        /// Unbuffered handle to the null device: one real system call per write, no disk I/O.
        /// </summary>
        public static FileStream OpenNullSink()
        {
            string path = OperatingSystem.IsWindows() ? @"\\.\NUL" : "/dev/null";
            return new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite, bufferSize: 0, FileOptions.Asynchronous);
        }

        /// <summary>
        /// Unbuffered handle to a file holding <paramref name="content"/>; reads are served from the OS page cache, one system call per read.
        /// The file is named by content hash and reused by later benchmark processes, so no process measures while a freshly
        /// written file is still being flushed or scanned.
        /// </summary>
        public static FileStream OpenUnbufferedSource(byte[] content)
        {
            string hash = Convert.ToHexString(SHA256.HashData(content))[..16];
            string path = Path.Combine(Datasets.CacheDirectory, $"source-{hash}.zst");
            if (File.Exists(path) == false || new FileInfo(path).Length != content.Length)
            {
                Directory.CreateDirectory(Datasets.CacheDirectory);
                string temp = path + "." + Environment.ProcessId + ".tmp";
                File.WriteAllBytes(temp, content);
                File.Move(temp, path, overwrite: true);
            }

            return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 0, FileOptions.Asynchronous);
        }
    }

    /// <summary>
    /// Counts the calls a wrapping stream makes into the stream below it.
    /// </summary>
    internal sealed class CountingStream : Stream
    {
        private readonly Stream _inner;

        public CountingStream(Stream inner)
        {
            _inner = inner;
        }

        public long Reads { get; private set; }
        public long Writes { get; private set; }
        public long Flushes { get; private set; }
        public long BytesRead { get; private set; }
        public long BytesWritten { get; private set; }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush()
        {
            Flushes++;
            _inner.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            Flushes++;
            return _inner.FlushAsync(cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            int read = _inner.Read(buffer);
            Reads++;
            BytesRead += read;
            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int read = await _inner.ReadAsync(buffer, cancellationToken);
            Reads++;
            BytesRead += read;
            return read;
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            Writes++;
            BytesWritten += buffer.Length;
            _inner.Write(buffer);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes++;
            BytesWritten += buffer.Length;
            return _inner.WriteAsync(buffer, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
