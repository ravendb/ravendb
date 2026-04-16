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
    public void CopyToPreservingSparseRegions_ShouldCreateHoleForZeroRunSpanningMultipleReadBuffers()
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
    public void CopyToPreservingSparseRegions_ShouldCreateHoleForZeroRunSpanningNonPageAlignedReads()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 17;
        var source = new byte[(2 + sparsePages) * pageSize];

        Fill(source, 0, pageSize, 11);
        Fill(source, (1 + sparsePages) * pageSize, pageSize, 12);

        using var input = new ChunkedReadStream(source, pageSize / 2);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal((long)source.Length - (long)sparsePages * pageSize, destination.TotalBytesWritten);
        Assert.Contains((long)sparsePages * pageSize, destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldPreserveTrailingSparseHoleAtEndOfStream()
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
    public void CopyToPreservingSparseRegions_ShouldPreserveTrailingSparseHoleAtEndOfStreamWithNonPageAlignedReads()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 9;
        var source = new byte[(1 + sparsePages) * pageSize];

        Fill(source, 0, pageSize, 13);

        using var input = new ChunkedReadStream(source, (pageSize / 2) + 37);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(pageSize, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldWriteZeroRunBelowSparseThreshold()
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
    public void CopyToPreservingSparseRegions_ShouldWriteZeroRunBelowSparseThresholdWithNonPageAlignedReads()
    {
        int pageSize = Constants.Storage.PageSize;
        int zeroPages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration - 1;
        var source = new byte[(2 + zeroPages) * pageSize];

        Fill(source, 0, pageSize, 14);
        Fill(source, (1 + zeroPages) * pageSize, pageSize, 15);

        using var input = new ChunkedReadStream(source, (pageSize / 3) + 29);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(source.Length, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldCreateHoleForZeroRunAtSparseThreshold()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration;
        var source = new byte[(2 + sparsePages) * pageSize];

        Fill(source, 0, pageSize, 16);
        Fill(source, (1 + sparsePages) * pageSize, pageSize, 17);

        using var input = new ChunkedReadStream(source, (pageSize / 4) + 17);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal((long)source.Length - (long)sparsePages * pageSize, destination.TotalBytesWritten);
        Assert.Contains((long)sparsePages * pageSize, destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldWritePartialPageTailAfterSparseHole()
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

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldWritePartialPageTailAfterSparseHoleWithNonPageAlignedReads()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 5;
        int tailLength = pageSize / 2;
        var source = new byte[((1 + sparsePages) * pageSize) + tailLength];

        Fill(source, 0, pageSize, 18);
        Fill(source, (1 + sparsePages) * pageSize, tailLength, 19);

        using var input = new ChunkedReadStream(source, (pageSize / 2) + 11);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(pageSize + tailLength, destination.TotalBytesWritten);
        Assert.Contains((long)sparsePages * pageSize, destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldUseFastPathForAllNonZeroContent()
    {
        int pageSize = Constants.Storage.PageSize;
        int pages = 30;
        var source = new byte[pages * pageSize];

        for (int i = 0; i < pages; i++)
            Fill(source, i * pageSize, pageSize, (byte)(i + 1));

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(source.Length, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldPreserveInternalZeroPagesInFastPathBuffer()
    {
        int pageSize = Constants.Storage.PageSize;
        int pages = 10;
        var source = new byte[pages * pageSize];

        Fill(source, 0 * pageSize, pageSize, 1);
        Fill(source, 1 * pageSize, pageSize, 2);
        Fill(source, 2 * pageSize, pageSize, 3);
        // pages 3, 4, 5 left as zeros (below sparse threshold, must be preserved as-is)
        Fill(source, 6 * pageSize, pageSize, 6);
        Fill(source, 7 * pageSize, pageSize, 7);
        Fill(source, 8 * pageSize, pageSize, 8);
        Fill(source, 9 * pageSize, pageSize, 9);

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(source.Length, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldHandleMultipleSparseRegions()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 11;
        int totalPages = 1 + sparsePages + 1 + sparsePages + 1;
        var source = new byte[totalPages * pageSize];

        Fill(source, 0, pageSize, 21);
        Fill(source, (1 + sparsePages) * pageSize, pageSize, 22);
        Fill(source, (1 + sparsePages + 1 + sparsePages) * pageSize, pageSize, 23);

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(3L * pageSize, destination.TotalBytesWritten);
        Assert.Equal(2, destination.ForwardSeekLengths.Count);
        Assert.All(destination.ForwardSeekLengths, len => Assert.Equal((long)sparsePages * pageSize, len));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldHandleAllZeroStreamWithPartialPageTail()
    {
        int pageSize = Constants.Storage.PageSize;
        int sparsePages = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 1;
        int tailLength = 100;
        var source = new byte[(sparsePages * pageSize) + tailLength];

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(tailLength, destination.TotalBytesWritten);
        Assert.Contains((long)sparsePages * pageSize, destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldHandleSourceSmallerThanOneBuffer()
    {
        int pageSize = Constants.Storage.PageSize;
        int pages = 5;
        var source = new byte[pages * pageSize];

        for (int i = 0; i < pages; i++)
            Fill(source, i * pageSize, pageSize, (byte)(i + 31));

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(source.Length, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CopyToPreservingSparseRegions_ShouldResolvePendingZeroRunBeforeFastPathWrite()
    {
        int pageSize = Constants.Storage.PageSize;
        const int bufferPages = 10; // must match DefaultBufferSize / pageSize
        var source = new byte[2 * bufferPages * pageSize];

        // Buffer 1: 3 non-zero pages, then 7 zero pages (below sparse threshold).
        // First byte non-zero, last byte zero -> slow path, carries 7-page zero run into buffer 2.
        Fill(source, 0 * pageSize, pageSize, 41);
        Fill(source, 1 * pageSize, pageSize, 42);
        Fill(source, 2 * pageSize, pageSize, 43);

        // Buffer 2: 10 non-zero pages -> fast path. Must resolve the 7-page pending zero run first.
        for (int i = 0; i < bufferPages; i++)
            Fill(source, (bufferPages + i) * pageSize, pageSize, (byte)(51 + i));

        using var input = new MemoryStream(source);
        using var destination = new RecordingSparseDestination();

        input.CopyToPreservingSparseRegions(destination, onProgress: null, CancellationToken.None);

        Assert.Equal(source.Length, destination.Length);
        Assert.Equal(source, destination.ToArray());
        Assert.Equal(source.Length, destination.TotalBytesWritten);
        Assert.Empty(destination.ForwardSeekLengths);
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

    private sealed class ChunkedReadStream : Stream
    {
        private readonly MemoryStream _inner;
        private readonly int _maxReadSize;

        public ChunkedReadStream(byte[] buffer, int maxReadSize)
        {
            _inner = new MemoryStream(buffer);
            _maxReadSize = maxReadSize;
        }

        public override bool CanRead => _inner.CanRead;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush()
        {
            _inner.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _inner.Read(buffer, offset, Math.Min(count, _maxReadSize));
        }

        public override int Read(Span<byte> buffer)
        {
            return _inner.Read(buffer[..Math.Min(buffer.Length, _maxReadSize)]);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _inner.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _inner.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();

            base.Dispose(disposing);
        }
    }
}
