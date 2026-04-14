using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using FastTests;
using Tests.Infrastructure;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Impl.FreeSpace;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_26344_StreamExtensionsSparseCopy(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparsity_ShouldCreateHoleForZeroRunSpanningMultipleReadBuffers()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 17;
        var source = new byte[(2 + sparsePages) * pageSize];

        Fill(source, 0, pageSize, 1);
        Fill(source, (1 + sparsePages) * pageSize, pageSize, 2);

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal((long)source.Length - (long)sparsePages * pageSize, destination.TotalBytesWritten);
        Assert.Contains((long)sparsePages * pageSize, destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparsity_ShouldPreserveTrailingSparseHoleAtEndOfStream()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 9;
        var source = new byte[(1 + sparsePages) * pageSize];

        Fill(source, 0, pageSize, 3);

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(pageSize, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparsity_ShouldWriteZeroRunBelowSparseThreshold()
    {
        int pageSize = Constants.Storage.PageSize;
        int zeroPages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration - 1;
        var source = new byte[(2 + zeroPages) * pageSize];

        Fill(source, 0, pageSize, 4);
        Fill(source, (1 + zeroPages) * pageSize, pageSize, 5);

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(source.Length, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparsity_ShouldWritePartialPageTailAfterSparseHole()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 5;
        int tailLength = pageSize / 2;
        var source = new byte[((1 + sparsePages) * pageSize) + tailLength];

        Fill(source, 0, pageSize, 6);
        Fill(source, (1 + sparsePages) * pageSize, tailLength, 7);

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(pageSize + tailLength, destination.TotalBytesWritten);
        Assert.Contains((long)sparsePages * pageSize, destination.ForwardSeekLengths);
    }

    private static void Fill(byte[] buffer, int offset, int length, byte value)
    {
        new Span<byte>(buffer, offset, length).Fill(value);
    }

    private sealed class RecordingSparseDestination : Stream
    {
        private readonly MemoryStream _inner = new();

        public List<long> ForwardSeekLengths { get; } = new();

        public long TotalBytesWritten { get; private set; }

        public override bool CanRead => _inner.CanRead;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => _inner.CanWrite;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public byte[] ToArray()
        {
            return _inner.ToArray();
        }

        public override void Flush()
        {
            _inner.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _inner.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long current = _inner.Position;
            long newPosition = _inner.Seek(offset, origin);
            if (newPosition > current)
                ForwardSeekLengths.Add(newPosition - current);

            return newPosition;
        }

        public override void SetLength(long value)
        {
            _inner.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            TotalBytesWritten += count;
            _inner.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            TotalBytesWritten += buffer.Length;
            _inner.Write(buffer);
        }
    }
}
