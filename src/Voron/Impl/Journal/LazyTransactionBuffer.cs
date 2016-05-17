using System;
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
        private readonly IVirtualPager _lazyTransactionPager;


        public LazyTransactionBuffer(StorageEnvironmentOptions options)
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
                _lazyTransactionPager.WriteDirect((byte*)page, _lastUsedPage, 1);
                _lastUsedPage++;
            }
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

        public void WriteBufferToFile(JournalFile journalFile)
        {
            if (_firstPositionInJournalFile != null)
            {
                journalFile.WriteBuffer(_firstPositionInJournalFile.Value, _lazyTransactionPager.AcquirePagePointer(null, 0),
                    _lastUsedPage * _options.DataPager.PageSize);
            }

            _readTransaction?.Dispose();
            _firstPositionInJournalFile = null;
            _lastUsedPage = 0;
            _readTransaction = null;
        }

        public void Dispose()
        {
            _lazyTransactionPager?.Dispose();
        }


    }
}