using System;
using System.Threading;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Voron.Impl.Paging;

/// <summary>The file pages the kernel evicts from the page cache, and the file pages it holds, from /proc/vmstat.</summary>
internal sealed class PageCacheEvictions : IDisposable
{
    private readonly KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader _reader;
    private long _previousEvicted = -1;

    internal PageCacheEvictions(string path)
    {
        _reader = new KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader(path);
    }

    public static PageCacheEvictions TryCreate() => PlatformDetails.RunningOnLinux ? new PageCacheEvictions("/proc/vmstat") : null;

    /// <summary>
    /// The file pages evicted since the previous call (none on the first call), and the file pages in the page cache. False
    /// while another call reads.
    /// </summary>
    public bool TryRead(out long evicted, out long filePages)
    {
        evicted = 0;
        filePages = 0;
        if (Monitor.TryEnter(_reader) == false)
            return false;
        try
        {
            return Read(out evicted, out filePages);
        }
        finally
        {
            Monitor.Exit(_reader);
        }
    }

    private bool Read(out long evicted, out long filePages)
    {
        evicted = 0;
        filePages = 0;
        try
        {
            _reader.ReadFileIntoBuffer();
        }
        catch
        {
            return false;
        }

        filePages = _reader.ExtractNumericValueFromKeyValuePairsFormattedFile("nr_inactive_file "u8) +
                    _reader.ExtractNumericValueFromKeyValuePairsFormattedFile("nr_active_file "u8);
        // in the order of the file; pgsteal_file is in Linux 5.8 and later, pgsteal_kswapd and pgsteal_direct count anonymous pages too
        var stealKswapd = _reader.ExtractNumericValueFromKeyValuePairsFormattedFile("pgsteal_kswapd "u8);
        var stealDirect = _reader.ExtractNumericValueFromKeyValuePairsFormattedFile("pgsteal_direct "u8);
        var stealFile = _reader.ExtractNumericValueFromKeyValuePairsFormattedFile("pgsteal_file "u8);
        var evictedSinceBoot = stealFile > 0 ? stealFile : stealKswapd + stealDirect;

        if (_previousEvicted >= 0)
            evicted = Math.Max(0, evictedSinceBoot - _previousEvicted);
        _previousEvicted = evictedSinceBoot;
        return filePages > 0;
    }

    public void Dispose() => _reader.Dispose();
}
