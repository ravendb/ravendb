using Sparrow;
using Sparrow.Binary;
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
using Voron.Util;

namespace Voron.Impl.Scratch
{
    /// <summary>
    /// This class implements the page pool for in flight transaction information
    /// Pages allocated from here are expected to live after the write transaction that 
    /// created them. The pages will be kept around until the flush for the journals
    /// send them to the data file.
    /// 
    /// This class relies on external synchronization and is not meant to be used in multiple
    /// threads at the same time
    /// </summary>
    public unsafe class ScratchBufferPool : IDisposable
    {
        private const int InvalidScratchFileNumber = -1;

        // Immutable state. 
        private readonly long _sizeLimit;
        private readonly StorageEnvironmentOptions _options;

        // Local per scratch file potentially read delayed inconsistent (need guards). All must be modified atomically (but it wont necessarily require a memory barrier)
        private ScratchBufferItem _current;

        // Local writable state. Can perform multiple reads, but must never do multiple writes simultaneously.
        private int _currentScratchNumber = -1;
        private readonly ConcurrentDictionary<int, ScratchBufferItem> _scratchBuffers = new ConcurrentDictionary<int, ScratchBufferItem>(NumericEqualityComparer.Instance);


        public ScratchBufferPool(StorageEnvironment env)
        {
            _options = env.Options;
            _sizeLimit = env.Options.MaxScratchBufferSize;
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
            scratchPager.EnsureContinuous(0, (int)(Math.Max(_options.InitialFileSize ?? 0, _options.InitialLogFileSize) / _options.PageSize));

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

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));
            var size = Bits.NextPowerOf2(numberOfPages);

            PageFromScratchBuffer result;
            var current = _current;
            if (current.File.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
                return result;

            long sizeAfterAllocation;
            long oldestActiveTransaction = tx.Environment.OldestTransaction;

            if (_scratchBuffers.Count == 1)
            {
                sizeAfterAllocation = current.File.SizeAfterAllocation(size);
            }
            else
            {
                sizeAfterAllocation = size * tx.Environment.Options.PageSize;

                var scratchesToDelete = new List<int>();

                sizeAfterAllocation += current.File.ActivelyUsedBytes(oldestActiveTransaction);

                // determine how many bytes of older scratches are still in use
                foreach (var scratch in _scratchBuffers.Values)
                {
                    var bytesInUse = scratch.File.ActivelyUsedBytes(oldestActiveTransaction);

                    if (bytesInUse <= 0)
                    {
                        if (scratch != current)
                            scratchesToDelete.Add(scratch.Number);
                    }
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
            }

            if (sizeAfterAllocation >= (_sizeLimit * 3) / 4 && oldestActiveTransaction > current.OldestTransactionWhenFlushWasForced)
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
                                tx.Environment.ForceLogFlushToDataFile(tx);
                                current.OldestTransactionWhenFlushWasForced = oldestActiveTransaction;
                            }
                            catch (TimeoutException)
                            {
                                // we'll try next time
                            }
                            catch (InvalidJournalFlushRequestException)
                            {
                                // journals flushing already in progress
                            }
                        }
                    }
                }
            }

            if (sizeAfterAllocation > _sizeLimit)
            {
                var sp = Stopwatch.StartNew();

                // Our problem is that we don't have any available free pages, probably because
                // there are read transactions that are holding things open. We are going to see if
                // there are any free pages that _might_ be freed for us if we wait for a bit. The idea
                // is that we let the read transactions time to complete and do their work, at which point
                // we can continue running. It is possible that a long running read transaction
                // would in fact generate enough work for us to timeout, but hopefully we can avoid that.

                while (tx.IsLazyTransaction == false && // lazy transaction is holding a read tx that will stop this, nothing to do here
                    tx.Environment.Options.ManualFlushing == false &&
                    sp.ElapsedMilliseconds < tx.Environment.Options.ScratchBufferOverflowTimeout)
                {
                    if (current.File.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
                        return result;
                    Thread.Sleep(32);
                }

                sp.Stop();

                bool createNextFile = false;

                if (tx.IsLazyTransaction)
                {
                    // in lazy transaction when reaching full scratch buffer - we might still have continuous space, but because
                    // of high insertion rate - we reach scratch buffer full, so we will create new scratch anyhow

                    createNextFile = true;
                }
                else if (current.File.HasDiscontinuousSpaceFor(tx, size, _scratchBuffers.Count))
                {
                    // there is enough space for the requested allocation but the problem is its fragmentation
                    // so we will create a new scratch file and will allow to allocate new continuous range from there

                    createNextFile = true;
                }
                else if (_scratchBuffers.Count == 1 && current.File.Size < _sizeLimit &&
                        (current.File.ActivelyUsedBytes(oldestActiveTransaction) + size * tx.Environment.Options.PageSize) < _sizeLimit)
                {
                    // there is only one scratch file that hasn't reach the size limit yet and
                    // the number of bytes being in active use allows to allocate the requested size
                    // let it create a new file

                    createNextFile = true;
                }

                if (createNextFile)
                {
                    // We need to ensure that _current stays constant through the codepath until return. 
                    current = NextFile();

                    try
                    {
                        current.File.PagerState.AddRef();
                        tx.EnsurePagerStateReference(current.File.PagerState);

                        return current.File.Allocate(tx, numberOfPages, size);
                    }
                    finally
                    {
                        // That's why we update only after exiting. 
                        _current = current;
                    }
                }

                ThrowScratchBufferTooBig(tx, numberOfPages, size, oldestActiveTransaction, sizeAfterAllocation, sp, current);
            }

            // we don't have free pages to give out, need to allocate some
            result = current.File.Allocate(tx, numberOfPages, size);
            _options.OnScratchBufferSizeChanged(sizeAfterAllocation);

            return result;
        }

        private void ThrowScratchBufferTooBig(LowLevelTransaction tx, int numberOfPages, long size, long oldestActiveTransaction,
            long sizeAfterAllocation, Stopwatch sp, ScratchBufferItem current)
        {
            var debugInfoBuilder = new StringBuilder();
            var totalPages = tx.GetTransactionPages().Count;

            debugInfoBuilder.AppendFormat("Current transaction id: {0}\r\n", tx.Id);
            if ((totalPages + numberOfPages)*tx.Environment.Options.PageSize >= _sizeLimit/2)
            {
                debugInfoBuilder.Append("- - - - - - - - - - - - -\r\n");
                debugInfoBuilder.AppendFormat(
                    "This transaction is VERY big, and requires {0:##,###;;0} kb out of {1:##,###;;0} kb allows!\r\n",
                    ((totalPages + numberOfPages)*tx.Environment.Options.PageSize)/1024,
                    _sizeLimit/1024
                    );
                debugInfoBuilder.Append("- - - - - - - - - - - - -\r\n");
            }
            
            debugInfoBuilder.AppendFormat("Requested number of pages: {0} (adjusted size: {1} == {2:#,#;;0} KB)\r\n", numberOfPages,
                size, size * tx.Environment.Options.PageSize / 1024);
            debugInfoBuilder.AppendFormat("Total number of pages in tx: {0} (adjusted size: {1} == {2:#,#;;0} KB)\r\n", totalPages,
               totalPages, totalPages * tx.Environment.Options.PageSize / 1024);
            debugInfoBuilder.AppendFormat("Oldest active transaction: {0} (snapshot: {1})\r\n", tx.Environment.OldestTransaction,
                oldestActiveTransaction);
            debugInfoBuilder.AppendFormat("Oldest active transaction when flush was forced: {0}\r\n",
                current.OldestTransactionWhenFlushWasForced);
            debugInfoBuilder.AppendFormat("Next write transaction id: {0}\r\n", tx.Environment.NextWriteTransactionId + 1);

            debugInfoBuilder.AppendLine("Active transactions:");
            foreach (var activeTransaction in tx.Environment.ActiveTransactions)
            {
                debugInfoBuilder.AppendFormat("\tId: {0} - {1}\r\n", activeTransaction.Id, activeTransaction.Flags);
            }

            debugInfoBuilder.AppendLine("Scratch files usage:");
            foreach (var scratchBufferFile in _scratchBuffers.OrderBy(x => x.Key))
            {
                debugInfoBuilder.AppendFormat("\t{0} - size: {1:#,#;;0} KB, in active use: {2:#,#;;0} KB\r\n",
                    StorageEnvironmentOptions.ScratchBufferName(scratchBufferFile.Value.Number), scratchBufferFile.Value.File.Size / 1024,
                    scratchBufferFile.Value.File.ActivelyUsedBytes(oldestActiveTransaction) / 1024);
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

            debugInfoBuilder.AppendFormat("Compression buffer size: {0:#,#;;0} KB\r\n",
                tx.Environment.Journal.CompressionBufferSize / 1024);

            string debugInfo = debugInfoBuilder.ToString();

            string message = string.Format("Cannot allocate more space for the scratch buffer.\r\n" +
                                           "Current file size is:\t{0:#,#;;0} KB.\r\n" +
                                           "Requested size for current file:\t{1:#,#;;0} KB.\r\n" +
                                           "Requested total size for all files:\t{2:#,#;;0} KB.\r\n" +
                                           "Limit:\t\t\t{3:#,#;;0} KB.\r\n" +
                                           "Already flushed and waited for {4:#,#;;0} ms for read transactions to complete.\r\n" +
                                           "Do you have a long running read transaction executing?\r\n" +
                                           "Debug info:\r\n{5}",
                current.File.Size / 1024L,
                current.File.SizeAfterAllocation(size) / 1024L,
                sizeAfterAllocation / 1024L,
                _sizeLimit / 1024L,
                sp.ElapsedMilliseconds,
                debugInfo
                );

            throw new ScratchBufferSizeLimitException(message);
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
            _scratchBuffers.Clear();
        }

        private class ScratchBufferItem
        {
            public readonly int Number;
            public readonly ScratchBufferFile File;
            public long OldestTransactionWhenFlushWasForced;

            public ScratchBufferItem(int number, ScratchBufferFile file)
            {
                this.Number = number;
                this.File = file;
                this.OldestTransactionWhenFlushWasForced = -1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page ReadPage(LowLevelTransaction tx, int scratchNumber, long p, PagerState pagerState = null)
        {
            var item = _scratchBuffers[scratchNumber];

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.ReadPage(tx, p, pagerState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AcquirePagePointer(LowLevelTransaction tx, int scratchNumber, long p)
        {
            var item = _scratchBuffers[scratchNumber];

            ScratchBufferFile bufferFile = item.File;
            return bufferFile.AcquirePagePointer(tx, p);
        }

        public void BreakLargeAllocationToSeparatePages(PageFromScratchBuffer value)
        {
            var item = _scratchBuffers[value.ScratchFileNumber];
            item.File.BreakLargeAllocationToSeparatePages(value);
        }

        public long GetAvailablePagesCount()
        {
            return _current.File.NumberOfAllocatedPages - _current.File.AllocatedPagesUsedSize;
        }
    }
}