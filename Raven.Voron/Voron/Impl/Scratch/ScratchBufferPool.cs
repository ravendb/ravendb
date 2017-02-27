using Sparrow;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Voron.Exceptions;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Scratch
{
    /// <summary>
    /// This class implements the page pool for in flight transaction information
    /// Pages allocated from here are expected to live after the write transaction that 
    /// created them. The pages will be kept around until the flush for the journals
    /// send them to the data file.
    /// 
    /// This class relies on external synchronization and it's no thread safe.     
    /// </summary>
    public unsafe class ScratchBufferPool : IDisposable
    {
        private const int InvalidScratchFileNumber = -1;

        // Immutable state. 
        private readonly long sizeLimit;
        private readonly long sizePerScratchLimit;
        private readonly StorageEnvironmentOptions _options;
        
        // Local per scratch file potentially read delayed inconsistent (need guards). All must be modified atomically (but it wont necessarily require a memory barrier)
        private ScratchBufferItem _current;		     
        private ScratchBufferItem lastScratchBuffer = new ScratchBufferItem(InvalidScratchFileNumber, null);

        // Local writable state. Can perform multiple reads, but must never do multiple writes simultaneously.
        private int _currentScratchNumber = -1;
        private DateTime lastScratchBuffersCleanup = DateTime.MinValue;
        private readonly ConcurrentDictionary<int, ScratchBufferItem> _scratchBuffers = new ConcurrentDictionary<int, ScratchBufferItem>(NumericEqualityComparer.Instance);

        public ScratchBufferPool(StorageEnvironment env)
        {
            _options = env.Options;
            sizeLimit = env.Options.MaxScratchBufferSize;
            sizePerScratchLimit = env.Options.MaxSizePerScratchBufferFile;
            _current = NextFile();
        }

        public Dictionary<int, PagerState> GetPagerStatesOfAllScratches()
        {
            // This is not risky anymore, but the caller must understand this is a monotonically incrementing snapshot. 
            return _scratchBuffers.ToDictionary(x => x.Key, y => y.Value.File.PagerState, NumericEqualityComparer.Instance);
        }

        internal long GetNumberOfAllocations(int scratchNumber)
        {
            // While used only in tests, there is no multithread risk. 
            return _scratchBuffers[scratchNumber].File.NumberOfAllocations;
        }

        private ScratchBufferItem NextFile()
        {           
            _currentScratchNumber++; 

            var scratchPager = _options.CreateScratchPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber));
            scratchPager.AllocateMorePages(null, Math.Max(_options.InitialFileSize ?? 0, _options.InitialLogFileSize));

            var scratchFile = new ScratchBufferFile(scratchPager, _currentScratchNumber);
            var item = new ScratchBufferItem(scratchFile.Number, scratchFile);

            _scratchBuffers.TryAdd(item.Number, item);

            return item;
        }

        public PagerState GetPagerState(int scratchNumber)
        {
            // Not thread-safe but only called by a single writer.
            var bufferFile = _scratchBuffers[scratchNumber].File;
            return bufferFile.PagerState;
        }

        public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages)
        {
            if (tx == null)
                throw new ArgumentNullException("tx");

            var size = Utils.NearestPowerOfTwo(numberOfPages);

            PageFromScratchBuffer result;

            var current = _current;
            var currentFile = current.File;
            var oldestActiveTransaction = tx.Environment.PossibleOldestReadTransaction;
            if (currentFile.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
            {
                TryDeletingInactiveScratchBuffers(current, oldestActiveTransaction, ignoreLastCleanup: false);
                return result;
            }

            TryDeletingInactiveScratchBuffers(current, oldestActiveTransaction, ignoreLastCleanup: true);

            var sizeAfterAllocation = GetTotalActivelyUsedBytes(size * AbstractPager.PageSize, oldestActiveTransaction);
            if (sizeAfterAllocation >= (sizeLimit*3)/4 && oldestActiveTransaction > current.OldestTransactionWhenFlushWasForced)
            {
                // we may get recursive flushing, so we want to avoid it
                if (tx.Environment.Journal.Applicator.IsCurrentThreadInFlushOperation == false)
                {
                    // We are starting to force a flush to free scratch pages. We are doing it at this point (80% of the max scratch size)
                    // to make sure that next transactions will be able to allocate pages that we are going to free in the current transaction.
                    // Important notice: all pages freed by this run will get ValidAfterTransactionId == tx.Id (so only next ones can use it)

                    bool flushLockTaken = false;
                    using (tx.Environment.Journal.Applicator.TryTakeFlushingLock(ref flushLockTaken))
                    {
                        if (flushLockTaken) // if we are already flushing, we don't need to force a flush
                        {
                            try
                            {
                                tx.Environment.ForceLogFlushToDataFile(tx, allowToFlushOverwrittenPages: true);
                                current.OldestTransactionWhenFlushWasForced = oldestActiveTransaction;
                            }
                            catch (TimeoutException)
                            {
                                // we'll try next time
                            }
                            catch (InvalidJournalFlushRequest)
                            {
                                // journals flushing already in progress
                            }
                        }
                    }
                }
            }

            if (currentFile.Size > sizePerScratchLimit)
            {
                // limit every scratch file size
                return CreateNextFile(tx, numberOfPages, size);
            }

            if (sizeAfterAllocation > sizeLimit)
            {
                var sp = Stopwatch.StartNew();

                // Our problem is that we don't have any available free pages, probably because
                // there are read transactions that are holding things open. We are going to see if
                // there are any free pages that _might_ be freed for us if we wait for a bit. The idea
                // is that we let the read transactions time to complete and do their work, at which point
                // we can continue running. It is possible that a long running read transaction
                // would in fact generate enough work for us to timeout, but hopefully we can avoid that.

                while (sp.ElapsedMilliseconds < tx.Environment.Options.ScratchBufferOverflowTimeout)
                {
                    if (currentFile.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
                        return result;

                    Thread.Sleep(32);
                }

                sp.Stop();

                var createNextFile = false;
                sizeAfterAllocation = GetTotalActivelyUsedBytes(size * AbstractPager.PageSize, tx.Environment.PossibleOldestReadTransaction);
                if (sizeAfterAllocation <= sizeLimit)
                {
                    // some long running transactions have finished

                    createNextFile = true;
                }
                else if (currentFile.HasDiscontinuousSpaceFor(tx, size, _scratchBuffers.Count))
                {
                    // there is enough space for the requested allocation but the problem is its fragmentation
                    // so we will create a new scratch file and will allow to allocate new continuous range from there

                    createNextFile = true;
                }

                if (createNextFile)
                {
                    return CreateNextFile(tx, numberOfPages, size);
                }

                var debugInfoBuilder = new StringBuilder();

                debugInfoBuilder.AppendFormat("Current transaction id: {0}\r\n", tx.Id);
                debugInfoBuilder.AppendFormat("Requested number of pages: {0} (adjusted size: {1} == {2:#,#;;0} KB)\r\n", numberOfPages, size, size * AbstractPager.PageSize / 1024);
                debugInfoBuilder.AppendFormat("Oldest active transaction: {0} (snapshot: {1})\r\n", tx.Environment.OldestTransaction, oldestActiveTransaction);
                debugInfoBuilder.AppendFormat("Possible oldest active transaction: {0}\r\n", tx.Environment.PossibleOldestReadTransaction);
                debugInfoBuilder.AppendFormat("Oldest active transaction when flush was forced: {0}\r\n", current.OldestTransactionWhenFlushWasForced);
                debugInfoBuilder.AppendFormat("Next write transaction id: {0}\r\n", tx.Environment.NextWriteTransactionId + 1);

                debugInfoBuilder.AppendLine("Active transactions:");
                foreach (var activeTransaction in tx.Environment.ActiveTransactions)
                {
                    debugInfoBuilder.AppendFormat("\tId: {0} - {1}\r\n", activeTransaction.Id, activeTransaction.Flags);
                }

                debugInfoBuilder.AppendLine("Scratch files usage:");
                foreach (var scratchBufferItem in _scratchBuffers.Values.OrderBy(x => x.Number))
                {
                    debugInfoBuilder.AppendFormat("\t{0} - size: {1:#,#;;0} KB, in active use: {2:#,#;;0} KB\r\n", StorageEnvironmentOptions.ScratchBufferName(scratchBufferItem.File.Number), scratchBufferItem.File.Size / 1024, scratchBufferItem.File.ActivelyUsedBytes(oldestActiveTransaction) / 1024);
                }

                debugInfoBuilder.AppendLine("Most available free pages:");
                foreach (var scratchBufferFile in _scratchBuffers.OrderBy(x => x.Key))
                {
                    debugInfoBuilder.AppendFormat("\t{0}\r\n", StorageEnvironmentOptions.ScratchBufferName(scratchBufferFile.Value.Number));

                    foreach (var freePage in scratchBufferFile.Value.File.GetMostAvailableFreePagesBySize())
                    {
                        debugInfoBuilder.AppendFormat("\t\tSize:{0}, ValidAfterTransactionId: {1}\r\n", freePage.Key, freePage.Value);
                    }
                }

                debugInfoBuilder.AppendFormat("Compression buffer size: {0:#,#;;0} KB\r\n", tx.Environment.Journal.CompressionBufferSize / 1024);

                string debugInfo = debugInfoBuilder.ToString();

                string message = string.Format("Cannot allocate more space for the scratch buffer.\r\n" +
                                               "Current file size is:\t{0:#,#;;0} KB.\r\n" +
                                               "Requested size for current file:\t{1:#,#;;0} KB.\r\n" +
                                               "Requested total size for all files:\t{2:#,#;;0} KB.\r\n" +
                                               "Limit:\t\t\t{3:#,#;;0} KB.\r\n" +
                                               "Already flushed and waited for {4:#,#;;0} ms for read transactions to complete.\r\n" +
                                               "Do you have a long running read transaction executing?\r\n" + 
                                               "Debug info:\r\n{5}",
                    currentFile.Size / 1024L,
                    currentFile.SizeAfterAllocation(size) / 1024L,
                    sizeAfterAllocation / 1024L,
                    sizeLimit / 1024L,
                    sp.ElapsedMilliseconds,
                    debugInfo
                    );

                throw new ScratchBufferSizeLimitException(message);
            }

            // we don't have free pages to give out, need to allocate some
            result = currentFile.Allocate(tx, numberOfPages, size);
            _options.OnScratchBufferSizeChanged(sizeAfterAllocation);

            return result;
        }

        private void TryDeletingInactiveScratchBuffers(ScratchBufferItem current, long oldestActiveTransaction, bool ignoreLastCleanup)
        {
            if (_scratchBuffers.Count <= 1)
                return;

            if (ignoreLastCleanup == false && (DateTime.UtcNow - lastScratchBuffersCleanup).TotalMinutes < 10)
                return;

            var scratchesToDelete = new List<int>();

            // determine how many bytes of older scratches are still in use (at least when this snapshot is taken)
            foreach (var scratch in _scratchBuffers.Values)
            {
                if (scratch == current)
                    continue;

                if (scratch.File.HasActivelyUsedBytes(oldestActiveTransaction))
                    continue;

                scratchesToDelete.Add(scratch.Number);
            }

            // delete inactive scratches
            foreach (var scratchNumber in scratchesToDelete)
            {
                ScratchBufferItem scratchBufferToRemove;
                if (_scratchBuffers.TryRemove(scratchNumber, out scratchBufferToRemove))
                {
                    scratchBufferToRemove.File.Dispose();
                }
            }

            lastScratchBuffersCleanup = DateTime.UtcNow;
        }

        private long GetTotalActivelyUsedBytes(long requestedSize, long oldestActiveTransaction)
        {
            var sizeAfterAllocation = requestedSize;

            foreach (var scratch in _scratchBuffers.Values)
            {
                sizeAfterAllocation += scratch.File.ActivelyUsedBytes(oldestActiveTransaction);
            }

            return sizeAfterAllocation;
        }

        private PageFromScratchBuffer CreateNextFile(Transaction tx, int numberOfPages, long size)
        {
            // We need to ensure that _current stays constant through the codepath until return. 
            var current = NextFile();

            try
            {
                tx.EnsurePagerStateReference(_current.File.PagerState);

                return current.File.Allocate(tx, numberOfPages, size);
            }
            finally
            {
                // That's why we update only after exiting. 
                _current = current;
            }
        }

        public void Free(int scratchNumber, long page, long asOfTxId)
        {
            var scratch = _scratchBuffers[scratchNumber];
            scratch.File.Free(page, asOfTxId);           
        }

        public void Dispose()
        {
            foreach (var scratch in _scratchBuffers)
            {
                scratch.Value.File.Dispose();
            }
        }

        private class ScratchBufferItem
        {
            public readonly int Number;
            public readonly ScratchBufferFile File;
            public long OldestTransactionWhenFlushWasForced;

            public ScratchBufferItem(int number, ScratchBufferFile file)
            {
                Number = number;
                File = file;
                OldestTransactionWhenFlushWasForced = -1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page ReadPage(Transaction tx, int scratchNumber, long p, PagerState pagerState = null)
        {
            ScratchBufferItem item = lastScratchBuffer;
            if (item.Number != scratchNumber)
                item = _scratchBuffers[scratchNumber];

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.ReadPage(tx, p, pagerState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AcquirePagePointer(Transaction tx, int scratchNumber, long p)
        {
            ScratchBufferItem item = lastScratchBuffer;
            if (item.Number != scratchNumber)
                item = _scratchBuffers[scratchNumber];

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.AcquirePagePointer(tx, p);       
        }

        public ScratchBufferPoolInfo InfoForDebug(long oldestActiveTransaction)
        {
            var currentFile = _current.File;
            var scratchBufferPoolInfo = new ScratchBufferPoolInfo
            {
                OldestActiveTransaction = oldestActiveTransaction,
                NumberOfScratchFiles = _scratchBuffers.Count,
                CurrentFileSizeInMB = currentFile.Size / 1024L / 1024L,
                PerScratchFileSizeLimitInMB = sizePerScratchLimit / 1024L / 1024L,
                TotalScratchFileSizeLimitInMB = sizeLimit / 1024L / 1024L
            };

            foreach (var scratchBufferItem in _scratchBuffers.Values.OrderBy(x => x.Number))
            {
                var current = _current;
                var scratchFileUsage = new ScratchFileUsage
                {
                    Name = StorageEnvironmentOptions.ScratchBufferName(scratchBufferItem.File.Number),
                    SizeInKB = scratchBufferItem.File.Size / 1024,
                    NumberOfAllocations = scratchBufferItem.File.NumberOfAllocations,
                    InActiveUseInKB = scratchBufferItem.File.ActivelyUsedBytes(oldestActiveTransaction) / 1024,
                    CanBeDeleted = scratchBufferItem != current && scratchBufferItem.File.HasActivelyUsedBytes(oldestActiveTransaction) == false,
                    TxIdAfterWhichLatestFreePagesBecomeAvailable = scratchBufferItem.File.TxIdAfterWhichLatestFreePagesBecomeAvailable
                };

                foreach (var freePage in scratchBufferItem.File.GetMostAvailableFreePagesBySize())
                {
                    scratchFileUsage.MostAvailableFreePages.Add(new MostAvailableFreePagesBySize
                    {
                        Size = freePage.Key,
                        ValidAfterTransactionId = freePage.Value
                    });
                }

                scratchBufferPoolInfo.ScratchFilesUsage.Add(scratchFileUsage);
            }

            return scratchBufferPoolInfo;
        }
    }
}
