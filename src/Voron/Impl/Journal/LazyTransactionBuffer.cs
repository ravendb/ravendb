using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Sparrow.Logging;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public unsafe class LazyTransactionBuffer : IDisposable
    {
        public bool HasDataInBuffer() => _firstPositionInJournalFile != null;

        private LowLevelTransaction _readTransaction;
        private long? _firstPositionInJournalFile;
        private int _lastUsed4Kbs;
        private readonly AbstractPager _lazyTransactionPager;
        private readonly TransactionPersistentContext _transactionPersistentContext;
        public int NumberOfPages { get; set; }
        private readonly Logger _log;
        private readonly StorageEnvironmentOptions _options;

        public LazyTransactionBuffer(StorageEnvironmentOptions options)
        {
            _lazyTransactionPager = options.CreateTemporaryBufferPager("lazy-transactions.buffer", options.InitialFileSize ?? options.InitialLogFileSize);
            _transactionPersistentContext = new TransactionPersistentContext(true);
            _log = LoggingSource.Instance.GetLogger<LazyTransactionBuffer>(options.BasePath);
            _options = options;
        }

        public void EnsureSize(int sizeInPages)
        {
            _lazyTransactionPager.EnsureContinuous(0, sizeInPages);
        }

        public void AddToBuffer(long position, CompressedPagesResult pages)
        {
            NumberOfPages += pages.NumberOfUncompressedPages;
            if (_firstPositionInJournalFile == null)
            {
                _firstPositionInJournalFile = position; // first lazy tx saves position to all lazy tx that comes afterwards
            }
            using (var writer = _lazyTransactionPager.BatchWriter())
            {
                writer.Write(_lastUsed4Kbs,
                    pages.NumberOf4Kbs,
                    pages.Base);
            }

            _lastUsed4Kbs += pages.NumberOf4Kbs;
        }

        public void EnsureHasExistingReadTransaction(LowLevelTransaction tx)
        {
            if (_readTransaction != null)
                return;
            // This transaction is required to prevent flushing of the data from the
            // scratch file to the data file before the lazy transaction buffers have
            // actually been flushed to the journal file
            _readTransaction = tx.Environment.NewLowLevelTransaction(_transactionPersistentContext, TransactionFlags.Read);
            tx.Environment.AllowDisposeWithLazyTransactionRunning(_readTransaction);
        }

        public void WriteBufferToFile(JournalFile journalFile, LowLevelTransaction tx)
        {
            if (_firstPositionInJournalFile != null)
            {
                using (var tempTx = new TempPagerTransaction())
                {
                    var numberOfPages = _lastUsed4Kbs / (Constants.Storage.PageSize/ (4 * Constants.Size.Kilobyte));
                    if ((_lastUsed4Kbs%(Constants.Storage.PageSize/(4*Constants.Size.Kilobyte))) != 0)
                        numberOfPages++;

                    _lazyTransactionPager.EnsureMapped(tempTx, 0, numberOfPages);
                    var src = _lazyTransactionPager.AcquirePagePointer(tempTx, 0);
                    var sp = Stopwatch.StartNew();
                    journalFile.JournalWriter.Write(_firstPositionInJournalFile.Value, src, _lastUsed4Kbs);
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Writing lazy transaction buffer with {_lastUsed4Kbs/4:#,#} kb took {sp.Elapsed}");
                    }
                    ZeroLazyTransactionBufferIfNeeded(tempTx);
                }
            }

            if (tx != null)
                tx.IsLazyTransaction = false;// so it will notify the flush thread it has work to do

            _readTransaction?.Dispose();
            _firstPositionInJournalFile = null;
            _lastUsed4Kbs = 0;
            _readTransaction = null;
            NumberOfPages = 0;
        }

        public void ZeroLazyTransactionBufferIfNeeded(IPagerLevelTransactionState tx)
        {
            if (_options.EncryptionEnabled == false)
                return;
            var lazyTxBufferSize = _lazyTransactionPager.NumberOfAllocatedPages * Constants.Storage.PageSize;
            var pagePointer = _lazyTransactionPager.AcquirePagePointer(tx, 0);
            Sodium.ZeroMemory(pagePointer, lazyTxBufferSize);
        }

        public void Dispose()
        {
            _lazyTransactionPager?.Dispose();
        }
    }
}