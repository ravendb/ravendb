using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public unsafe class LazyTransactionBuffer : IDisposable
    {
        public bool HasDataInBuffer() => _firstPositionInJournalFile != null;

        private LowLevelTransaction _readTransaction;
        private long? _firstPositionInJournalFile;
        private int _lastUsedPage;
        private readonly AbstractPager _lazyTransactionPager;
        private readonly TransactionPersistentContext _transactionPersistentContext;
        public int NumberOfPages { get; set; }

        public LazyTransactionBuffer(StorageEnvironmentOptions options)
        {
            _lazyTransactionPager = options.CreateScratchPager("lazy-transactions.buffer", options.InitialFileSize ?? options.InitialLogFileSize);
            _transactionPersistentContext = new TransactionPersistentContext(true);
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
            using (var writer = _lazyTransactionPager.BatchWriter())
            {
                writer.Write(_lastUsedPage,
                    pages.NumberOfPages,
                    pages.Base);

            }

            _lastUsedPage += pages.NumberOfPages;
        }

        public void EnsureHasExistingReadTransaction(LowLevelTransaction tx)
        {
            if (_readTransaction != null)
                return;
            // This transaction is required to prevent flushing of the data from the
            // scratch file to the data file before the lazy transaction buffers have
            // actually been flushed to the journal file
            _readTransaction = tx.Environment.NewLowLevelTransaction(_transactionPersistentContext, TransactionFlags.Read);
        }

        public void WriteBufferToFile(JournalFile journalFile, LowLevelTransaction tx)
        {
            if (_firstPositionInJournalFile != null)
            {
                using (var tempTx = new TempPagerTransaction())
                {
                    _lazyTransactionPager.EnsureMapped(tempTx, 0, _lastUsedPage);
                    var src = _lazyTransactionPager.AcquirePagePointer(tempTx, 0);
                    journalFile.JournalWriter.WritePages(_firstPositionInJournalFile.Value, src, _lastUsedPage);
                }
            }

            if (tx != null)
                tx.IsLazyTransaction = false;// so it will notify the flush thread it has work to do

            _readTransaction?.Dispose();
            _firstPositionInJournalFile = null;
            _lastUsedPage = 0;
            _readTransaction = null;
            NumberOfPages = 0;
        }

        public void Dispose()
        {
            _lazyTransactionPager?.Dispose();
        }
    }
}