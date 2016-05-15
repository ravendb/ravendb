using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Voron;
using Voron.Impl.Paging;
using Voron.Platform.Posix;
using Voron.Platform.Win32;
using Voron.Impl;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;

public unsafe class LazyTransactionBuffer : IDisposable
{
    public bool HasDataInBuffer() => _firstPositionInJournalFile != null;

    private long? _firstPositionInJournalFile;
    private int _lastUsedPage;
    private readonly StorageEnvironmentOptions _options;
    private readonly IVirtualPager _lazyTransactionPager;


    public LazyTransactionBuffer( StorageEnvironmentOptions options)
    {
        _options = options;
        _lazyTransactionPager = _options.CreateScratchPager("lazy-transactions.buffer");
    }

    public void EnsureSize(int sizeInPages)
    {
        _lazyTransactionPager.EnsureContinuous(0, sizeInPages);
    }

    public void AddToBuffer(long position, IntPtr[] pages)
    {
        if (_firstPositionInJournalFile == null)
        {
            _firstPositionInJournalFile = position; // first lazy tx saves position to all lazy tx that comes afterwards
        }

        foreach (var page in pages)
        {
            _lazyTransactionPager.WriteDirect((byte*)page, _lastUsedPage, _options.DataPager.PageSize);
            _lastUsedPage++;
        }
    }

    public void WriteBufferToFile(JournalFile journalFile)
    {
        if (_firstPositionInJournalFile != null)
        {
            journalFile.WriteBuffer(_firstPositionInJournalFile.Value, _lazyTransactionPager.AcquirePagePointer(0),
                _lastUsedPage*_options.DataPager.PageSize);
        }
        _firstPositionInJournalFile = null;
        _lastUsedPage = 0;
    }

    public void Dispose()
    {
        _lazyTransactionPager?.Dispose();
    }

    
}