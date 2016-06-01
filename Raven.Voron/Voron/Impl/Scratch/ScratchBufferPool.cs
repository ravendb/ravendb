using Sparrow;
using System;
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
    /// This class relies on external synchronization and is not meant to be used in multiple
    /// threads at the same time
    /// </summary>
    public unsafe class ScratchBufferPool : IDisposable
    {
        private readonly long _sizeLimit;
        private ScratchBufferFile _current;
        private StorageEnvironmentOptions _options;
        private int _currentScratchNumber = -1;
        private long _oldestTransactionWhenFlushWasForced = -1;
        private readonly Dictionary<int, ScratchBufferFile> _scratchBuffers = new Dictionary<int, ScratchBufferFile>(NumericEqualityComparer.Instance);

        public ScratchBufferPool(StorageEnvironment env)
        {
            _options = env.Options;
            _sizeLimit = env.Options.MaxScratchBufferSize;
            _current = NextFile();
        }

        public Dictionary<int, PagerState> GetPagerStatesOfAllScratches()
        {
            return _scratchBuffers.ToDictionary(x => x.Key, y => y.Value.PagerState, NumericEqualityComparer.Instance);
        }

        internal long GetNumberOfAllocations(int scratchNumber)
        {
            return _scratchBuffers[scratchNumber].NumberOfAllocations;
        }

        private ScratchBufferFile NextFile()
        {
            _currentScratchNumber++;
            var scratchPager = _options.CreateScratchPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber));
            scratchPager.AllocateMorePages(null, Math.Max(_options.InitialFileSize ?? 0, _options.InitialLogFileSize));

            var scratchFile = new ScratchBufferFile(scratchPager, _currentScratchNumber);
            _scratchBuffers.Add(_currentScratchNumber, scratchFile);

            _oldestTransactionWhenFlushWasForced = -1;

            return scratchFile;
        }

        public PagerState GetPagerState(int scratchNumber)
        {
            return _scratchBuffers[scratchNumber].PagerState;
        }

        public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages)
        {
            if (tx == null)
                throw new ArgumentNullException("tx");
            var size = Utils.NearestPowerOfTwo(numberOfPages);

            PageFromScratchBuffer result;
            if (_current.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
                return result;

            long sizeAfterAllocation;
            long oldestActiveTransaction = tx.Environment.OldestTransaction;

            if (_scratchBuffers.Count == 1)
            {
                sizeAfterAllocation = _current.SizeAfterAllocation(size);
            }
            else
            {
                sizeAfterAllocation = size * AbstractPager.PageSize;

                var scratchesToDelete = new List<int>();

                // determine how many bytes of older scratches are still in use
                foreach (var scratch in _scratchBuffers.Values)
                {
                    var bytesInUse = scratch.ActivelyUsedBytes(oldestActiveTransaction);

                    if (bytesInUse > 0)
                        sizeAfterAllocation += bytesInUse;
                    else
                    {
                        if(scratch != _current)
                            scratchesToDelete.Add(scratch.Number);
                    }
                }

                // delete inactive scratches
                foreach (var scratchNumber in scratchesToDelete)
                {
                    var scratchBufferFile = _scratchBuffers[scratchNumber];
                    _scratchBuffers.Remove(scratchNumber);
                    scratchBufferFile.Dispose();
                }
            }

            if (sizeAfterAllocation >= (_sizeLimit*3)/4 && oldestActiveTransaction > _oldestTransactionWhenFlushWasForced)
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
                                _oldestTransactionWhenFlushWasForced = oldestActiveTransaction;
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

            if (sizeAfterAllocation > _sizeLimit)
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
                    if (_current.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
                        return result;
                    Thread.Sleep(32);
                }

                sp.Stop();

                bool createNextFile = false;

                if (_current.HasDiscontinuousSpaceFor(tx, size, _scratchBuffers.Count))
                {
                    // there is enough space for the requested allocation but the problem is its fragmentation
                    // so we will create a new scratch file and will allow to allocate new continuous range from there

                    createNextFile = true;
                }
                else if (_scratchBuffers.Count == 1 && _current.Size < _sizeLimit && 
                        (_current.ActivelyUsedBytes(oldestActiveTransaction) + size * AbstractPager.PageSize) < _sizeLimit)
                {
                    // there is only one scratch file that hasn't reach the size limit yet and
                    // the number of bytes being in active use allows to allocate the requested size
                    // let it create a new file

                    createNextFile = true;
                }

                if (createNextFile)
                {
                    _current = NextFile();
                    
                    tx.EnsurePagerStateReference(_current.PagerState);

                    return _current.Allocate(tx, numberOfPages, size);
                }

                var debugInfoBuilder = new StringBuilder();

                debugInfoBuilder.AppendFormat("Current transaction id: {0}\r\n", tx.Id);
                debugInfoBuilder.AppendFormat("Requested number of pages: {0} (adjusted size: {1} == {2:#,#;;0} KB)\r\n", numberOfPages, size, size * AbstractPager.PageSize / 1024);
                debugInfoBuilder.AppendFormat("Oldest active transaction: {0} (snapshot: {1})\r\n", tx.Environment.OldestTransaction, oldestActiveTransaction);
                debugInfoBuilder.AppendFormat("Oldest active transaction when flush was forced: {0}\r\n", _oldestTransactionWhenFlushWasForced);
                debugInfoBuilder.AppendFormat("Next write transaction id: {0}\r\n", tx.Environment.NextWriteTransactionId + 1);

                debugInfoBuilder.AppendLine("Active transactions:");
                foreach (var activeTransaction in tx.Environment.ActiveTransactions)
                {
                    debugInfoBuilder.AppendFormat("\tId: {0} - {1}\r\n", activeTransaction.Id, activeTransaction.Flags);
                }

                debugInfoBuilder.AppendLine("Scratch files usage:");
                foreach (var scratchBufferFile in _scratchBuffers.OrderBy(x => x.Key))
                {
                    debugInfoBuilder.AppendFormat("\t{0} - size: {1:#,#;;0} KB, in active use: {2:#,#;;0} KB\r\n", StorageEnvironmentOptions.ScratchBufferName(scratchBufferFile.Value.Number), scratchBufferFile.Value.Size / 1024, scratchBufferFile.Value.ActivelyUsedBytes(oldestActiveTransaction) / 1024);
                }

                debugInfoBuilder.AppendLine("Most available free pages:");
                foreach (var scratchBufferFile in _scratchBuffers.OrderBy(x => x.Key))
                {
                    debugInfoBuilder.AppendFormat("\t{0}\r\n", StorageEnvironmentOptions.ScratchBufferName(scratchBufferFile.Value.Number));

                    foreach (var freePage in scratchBufferFile.Value.GetMostAvailableFreePagesBySize())
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
                    _current.Size / 1024L,
                    _current.SizeAfterAllocation(size) / 1024L,
                    sizeAfterAllocation / 1024L,
                    _sizeLimit / 1024L,
                    sp.ElapsedMilliseconds,
                    debugInfo
                    );

                throw new ScratchBufferSizeLimitException(message);
            }

            // we don't have free pages to give out, need to allocate some
            result = _current.Allocate(tx, numberOfPages, size);
            _options.OnScratchBufferSizeChanged(sizeAfterAllocation);

            return result;
        }

        public void Free(int scratchNumber, long page, long asOfTxId)
        {
            _scratchBuffers[scratchNumber].Free(page, asOfTxId);
        }

        public void Dispose()
        {
            foreach (var scratchBufferFile in _scratchBuffers)
            {
                scratchBufferFile.Value.Dispose();
            }
        }

        private class ScratchBufferCacheItem
        {
            public readonly int Number;
            public readonly ScratchBufferFile File;

            public ScratchBufferCacheItem(int number, ScratchBufferFile file)
            {
                this.Number = number;
                this.File = file;
            }
        }

        private const int InvalidScratchFileNumber = -1;
        private ScratchBufferCacheItem lastScratchBuffer = new ScratchBufferCacheItem( InvalidScratchFileNumber, null );

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page ReadPage(Transaction tx, int scratchNumber, long p, PagerState pagerState = null)
        {
            ScratchBufferFile bufferFile;
            ScratchBufferCacheItem item = lastScratchBuffer;
            if ( item.Number == scratchNumber )
            {
                bufferFile = item.File;
            }
            else
            {
                bufferFile = _scratchBuffers[scratchNumber];
                lastScratchBuffer = new ScratchBufferCacheItem( scratchNumber, bufferFile );
            }

            return bufferFile.ReadPage(tx, p, pagerState);
        }

        public byte* AcquirePagePointer(Transaction tx, int scratchNumber, long p)
        {
            return _scratchBuffers[scratchNumber].AcquirePagePointer(tx, p);
        }
    }
}
