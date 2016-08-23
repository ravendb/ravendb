using System;
using System.Diagnostics;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public unsafe class LazyTransactionBuffer : IDisposable
    {
        public bool HasDataInBuffer() => _firstPositionInJournalFile != null;

        private LowLevelTransaction _readTransaction;
        private long? _firstPositionInJournalFile;
        private int _lastUsedPage;
        private readonly StorageEnvironmentOptions _options;
        private readonly AbstractPager _lazyTransactionPager;
        public int NumberOfPages { get; set; }

        public LazyTransactionBuffer(StorageEnvironmentOptions options)
        {
            _options = options;
            _lazyTransactionPager = _options.CreateScratchPager("lazy-transactions.buffer");
        }

        public void EnsureSize(int sizeInPages)
        {
            _lazyTransactionPager.EnsureContinuous(0, sizeInPages);
        }

        public void AddToBuffer(long position, CompressedPagesResult pages, int uncompressedPageCount)
        {
            NumberOfPages += uncompressedPageCount;
            if (_firstPositionInJournalFile == null)
            {
                _firstPositionInJournalFile = position; // first lazy tx saves position to all lazy tx that comes afterwards
            }
            _lazyTransactionPager.WriteDirect(pages.Base, _lastUsedPage, pages.NumberOfPages);

            _lastUsedPage += pages.NumberOfPages;
        }

        public void EnsureHasExistingReadTransaction(LowLevelTransaction tx)
        {
            if (_readTransaction != null)
                return;
            // This transaction is required to prevent flushing of the data from the 
            // scratch file to the data file before the lazy transaction buffers have 
            // actually been flushed to the journal file
            _readTransaction = tx.Environment.NewLowLevelTransaction(TransactionFlags.Read);
        }

        public int WriteBufferToFile(JournalFile journalFile, LowLevelTransaction tx)
        {
            int ioRate = 0;
            if (_firstPositionInJournalFile != null)
            {
                var sp = Stopwatch.StartNew();
                journalFile.JournalWriter.WritePages(_firstPositionInJournalFile.Value, _lazyTransactionPager.AcquirePagePointer(null, 0),
    _lastUsedPage);

                sp.Stop();

                int elapsed = (int)sp.ElapsedTicks;
                if (elapsed == 0)
                    elapsed = 1; // prevent dev by zero

                ioRate = (_lastUsedPage * tx.Environment.Options.PageSize) / elapsed;
            }

            if (tx != null)
                tx.IsLazyTransaction = false;// so it will notify the flush thread it has work to do

            _readTransaction?.Dispose();
            _firstPositionInJournalFile = null;
            _lastUsedPage = 0;
            _readTransaction = null;
            NumberOfPages = 0;

            return ioRate;
        }

        public void Dispose()
        {
            _lazyTransactionPager?.Dispose();
        }
    }
}