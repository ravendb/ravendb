using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server;
using Sparrow.Server.Exceptions;
using Sparrow.Threading;
using Voron.Impl.Paging;
using Voron.Util;
using Constants = Voron.Global.Constants;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Server.Logging;
using Sparrow.Server.LowMemory;
using Voron.Logging;

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
    public sealed class ScratchBufferPool : ILowMemoryHandler, IDisposable
    {
        private readonly StorageEnvironment _env;
        // Immutable state. 
        private readonly StorageEnvironmentOptions _options;

        // Local per scratch file potentially read delayed inconsistent (need guards). All must be modified atomically (but it wont necessarily require a memory barrier)
        internal ScratchBufferItem _current;

        // Local writable state. Can perform multiple reads, but must never do multiple writes simultaneously.
        private int _currentScratchNumber = -1;

        private readonly ConcurrentDictionary<int, ScratchBufferItem> _scratchBuffers = new();
        private readonly ConcurrentDictionary<Pager, ScratchBufferItem> _scratchBuffersByPager = new();

        private readonly LinkedList<ScratchBufferItem> _recycleArea = new();

        private readonly DisposeOnce<ExceptionRetry> _disposeOnceRunner;

        private long _lastLowMemoryEventTicks = 0;
        private readonly long _lowMemoryIntervalTicks = TimeSpan.FromMinutes(3).Ticks;
        private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();

        private readonly ScratchSpaceUsageMonitor _scratchSpaceMonitor; // it tracks total size of all scratches (active and recycled)
        private readonly RavenLogger _logger;

        public long NumberOfScratchBuffers => _scratchBuffers.Count;

        public ScratchBufferPool(StorageEnvironment env)
        {
            _logger = RavenLogManager.Instance.GetLoggerForVoron<ScratchBufferPool>(env.Options, env.ToString());

            _disposeOnceRunner = new DisposeOnce<ExceptionRetry>(() =>
            {
                foreach (var scratch in _scratchBuffers)
                {
                    scratch.Value.File.Dispose();
                    _scratchSpaceMonitor.Decrease(scratch.Value.File.NumberOfAllocatedPages * Constants.Storage.PageSize);
                }

                _scratchBuffers.Clear();
                _scratchBuffersByPager.Clear();

                while (_recycleArea.First != null)
                {
                    var recycledScratch = _recycleArea.First.Value;

                    if (recycledScratch.File.IsDisposed == false)
                    {
                        recycledScratch.File.Dispose();
                        _scratchSpaceMonitor.Decrease(recycledScratch.File.NumberOfAllocatedPages * Constants.Storage.PageSize);
                    }

                    _recycleArea.RemoveFirst();
                }

                _current = null;
            });

            _env = env;
            _options = env.Options;
            _scratchSpaceMonitor = env.Options.ScratchSpaceUsage;
            _current = NextFile(_options.InitialLogFileSize, null);

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        internal TimeSpan RecycledScratchFileTimeout { get; set; } = TimeSpan.FromMinutes(1);

        internal long GetNumberOfAllocations(int scratchNumber)
        {
            // while used only in tests, there is no multi thread risk. 
            return _scratchBuffers[scratchNumber].File.NumberOfAllocations;
        }

        private ScratchBufferItem NextFile(long minSize, long? requestedSize)
        {
            var current = _recycleArea.Last;
            var oldestTx = _env.PossibleOldestReadTransaction(null);
            while (current != null)
            {
                var recycled = current.Value;

                if (recycled.File.Size >= Math.Max(minSize, requestedSize ?? 0) &&
                    // even though this is in the recyle bin, there might still be some transactions looking at it
                    // so we have to make sure that this is really unused before actually reusing it
                    recycled.File.HasActivelyUsedBytes(oldestTx) == false)
                {
                    recycled.File.Reset();
                    recycled.RecycledAt = default(DateTime);
                    _recycleArea.Remove(current);
                    AddScratchBufferFile(recycled);

                    _scratchSpaceMonitor.Increase(recycled.File.NumberOfAllocatedPages * Constants.Storage.PageSize);

                    return recycled;
                }

                current = current.Previous;
            }
            
            _currentScratchNumber++;
            Pager scratchPager;
            Pager.State scratchPagerState;
            if (requestedSize != null)
            {
                try
                {
                    (scratchPager, scratchPagerState) =
                        _options.CreateTemporaryBufferPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber),
                            requestedSize.Value, encrypted: _options.Encryption.IsEnabled);
                }
                catch (Exception)
                {
                    // this can fail because of disk space issue, let us just ignore it
                    // we'll allocate the minimum amount in a bit anyway
                    return NextFile(minSize, null);
                }
            }
            else
            {
                (scratchPager, scratchPagerState) = _options.CreateTemporaryBufferPager(StorageEnvironmentOptions.ScratchBufferName(_currentScratchNumber),
                    Math.Max(_options.InitialLogFileSize, minSize),
                    encrypted: _options.Encryption.IsEnabled);
            }

            var scratchFile = new ScratchBufferFile(scratchPager, scratchPagerState, _currentScratchNumber);
            var item = new ScratchBufferItem(scratchFile.Number, scratchFile);

            AddScratchBufferFile(item);

            _scratchSpaceMonitor.Increase(item.File.NumberOfAllocatedPages * Constants.Storage.PageSize);

            return item;
        }

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages, long pageNumber, Page previousVersion)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));
            var size = Bits.PowerOf2(numberOfPages);

            var current = _current;

            if (current.File.TryGettingFromAllocatedBuffer(tx, numberOfPages, size, pageNumber, previousVersion, out PageFromScratchBuffer result))
                return result;

            // we can allocate from the end of the file directly
            if (current.File.LastUsedPage + size <= current.File.NumberOfAllocatedPages)
                return current.File.Allocate(tx, numberOfPages, size, pageNumber, previousVersion);

            if (current.File.Size < _options.MaxScratchBufferSize)
            {
                var numberOfPagesBeforeAllocate = current.File.NumberOfAllocatedPages;

                var page = current.File.Allocate(tx, numberOfPages, size, pageNumber, previousVersion);

                if (current.File.NumberOfAllocatedPages > numberOfPagesBeforeAllocate)
                    _scratchSpaceMonitor.Increase((current.File.NumberOfAllocatedPages - numberOfPagesBeforeAllocate) * Constants.Storage.PageSize);

                return page;
            }

            var minSize = numberOfPages * Constants.Storage.PageSize;
            var requestedSize = Math.Max(minSize, Math.Min(_current.File.Size * 2, _options.MaxScratchBufferSize));
            // We need to ensure that _current stays constant through the codepath until return. 
            current = NextFile(minSize, requestedSize);

            try
            {
                return current.File.Allocate(tx, numberOfPages, size, pageNumber, previousVersion);
            }
            finally
            {
                // That's why we update only after exiting. 
                _current = current;
            }
        }

        public void Free(LowLevelTransaction tx, int scratchNumber, long page)
        {
            var scratch = _scratchBuffers[scratchNumber];
            if (scratch.File.Free(tx, page))
            {
                MaybeRecycleFile(tx, scratch);
            }
        }

        private void MaybeRecycleFile(LowLevelTransaction tx, ScratchBufferItem scratch)
        {
            List<ScratchBufferFile> recycledScratchesToDispose = null;

            while (_recycleArea.First != null)
            {
                var recycledScratch = _recycleArea.First.Value;

                if (IsLowMemory() == false && 
                    DateTime.UtcNow - recycledScratch.RecycledAt <= RecycledScratchFileTimeout)
                    break;

                _recycleArea.RemoveFirst();

                if (recycledScratch.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction(tx)))
                {
                    // even though this was in the recycle area, there might still be some transactions looking at it
                    // so we cannot dispose it right now, the disposal will happen in RemoveInactiveScratches 
                    // when we are sure it's really no longer in use

                    continue;
                }

                if (_scratchBuffers.TryRemove(recycledScratch.Number, out var removedFile))
                {
                    _scratchBuffersByPager.TryRemove(removedFile.File.Pager, out _);
                }

                if (recycledScratchesToDispose == null)
                    recycledScratchesToDispose = new List<ScratchBufferFile>();

                recycledScratchesToDispose.Add(recycledScratch.File);
            }

            if (recycledScratchesToDispose != null)
            {
                foreach (var recycledScratch in recycledScratchesToDispose)
                {
                    recycledScratch.Dispose();

                    _scratchSpaceMonitor.Decrease(recycledScratch.NumberOfAllocatedPages * Constants.Storage.PageSize);
                }

                _forTestingPurposes?.ActionToCallDuringRemovalsOfRecycledScratchesRightAfterDisposingScratches?.Invoke();

            }

            if (scratch == _current)
            {
                if (scratch.File.Size <= _options.MaxScratchBufferSize)
                {
                    // we'll take the chance that no one is using us to reset the memory allocations
                    // and avoid fragmentation, we can only do that if no transaction is looking at us
                    if (scratch.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction(tx)) == false)
                        scratch.File.Reset();

                    return;
                }

                // this is the current one, but the size is too big, let us trim it
                var newCurrent = NextFile(_options.InitialLogFileSize, _options.MaxScratchBufferSize);
                _current = newCurrent;
            }

            TryRecycleScratchFile(scratch);
        }

        private void TryRecycleScratchFile(ScratchBufferItem scratch)
        {
            if (scratch.File.Size != _current.File.Size)
                return;

            if (IsLowMemory())
                return;

            scratch.RecycledAt = DateTime.UtcNow;
            _recycleArea.AddLast(scratch);
        }

        public void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        internal sealed class ScratchBufferItem(int number, ScratchBufferFile file)
        {
            public readonly int Number = number;
            public readonly ScratchBufferFile File = file;

            public DateTime RecycledAt;
        }

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddScratchBufferFile(ScratchBufferItem scratch)
        {
            RemoveInactiveScratches(scratch);

            _scratchBuffers.AddOrUpdate(scratch.Number, scratch, (_, __) => scratch);
            _scratchBuffersByPager.AddOrUpdate(scratch.File.Pager, scratch, (_, __) => scratch);
        }

        private int RemoveInactiveScratches(ScratchBufferItem except)
        {
            List<ScratchBufferFile> scratchesToDispose = null;

            foreach (var item in _scratchBuffers)
            {
                var scratchBufferItem = item.Value;

                if (scratchBufferItem.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction(null)) ||
                    scratchBufferItem == except ||
                    scratchBufferItem == _current)
                    continue;

                if (_scratchBuffers.TryRemove(scratchBufferItem.Number, out var removedFile) == false)
                    ThrowUnableToRemoveScratch(scratchBufferItem);
                _scratchBuffersByPager.TryRemove(removedFile.File.Pager, out _);

                if (_recycleArea.Contains(scratchBufferItem) == false)
                {
                    if (scratchesToDispose == null)
                        scratchesToDispose = new List<ScratchBufferFile>();
                    
                    scratchesToDispose.Add(scratchBufferItem.File);
                }
            }

            if (scratchesToDispose == null) 
                return 0;

            foreach (var scratch in scratchesToDispose)
            {
                scratch.Dispose();

                _forTestingPurposes?.ActionToCallDuringRemovalsOfInactiveScratchesRightAfterDisposingScratch?.Invoke();

                _scratchSpaceMonitor.Decrease(scratch.NumberOfAllocatedPages * Constants.Storage.PageSize);
            }

            return scratchesToDispose.Count;
        }

        [DoesNotReturn]
        private static void ThrowUnableToRemoveScratch(ScratchBufferItem scratchBufferItem)
        {
            throw new InvalidOperationException(
                $"Could not remove a scratch file from the scratch buffers collection. Number: {scratchBufferItem.Number}");
        }

        public void Cleanup()
        {
            if (_recycleArea.Count == 0 && _scratchBuffers.Count == 1)
                return;

            long txIdAllowingToReleaseOldScratches = -1;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var scratchBufferItem in _scratchBuffers)
            {
                if (scratchBufferItem.Value == _current)
                    continue;

                txIdAllowingToReleaseOldScratches = Math.Max(txIdAllowingToReleaseOldScratches,
                    scratchBufferItem.Value.File.TxIdAfterWhichLatestFreePagesBecomeAvailable);
            }

            ByteStringContext byteStringContext;
            try
            {
                byteStringContext = new ByteStringContext(SharedMultipleUseFlag.None);
            }
            catch (Exception e) when (e is OutOfMemoryException || e is EarlyOutOfMemoryException)
            {
                return;
            }

            try
            {
                while (_env.CurrentReadTransactionId <= txIdAllowingToReleaseOldScratches)
                {
                    // we've just flushed and had no more writes after that, let us bump id of next read transactions to ensure
                    // that nobody will attempt to read old scratches so we will be able to release more files

                    try
                    {
                        using (var tx = _env.NewLowLevelTransaction(new TransactionPersistentContext(),
                            TransactionFlags.ReadWrite, timeout: TimeSpan.FromMilliseconds(500), context: byteStringContext))
                        {
                            tx.ModifyPage(0);
                            tx.Commit();
                        }
                    }
                    catch (TimeoutException)
                    {
                        break;
                    }
                    catch (DiskFullException)
                    {
                        break;
                    }
                }

                IDisposable exitPreventNewTransactions = null;

                try
                {
                    // we need to ensure that no access to _recycleArea and _scratchBuffers will take place in the same time
                    // and only methods that access this are used within write transaction

                    using (_env.WriteTransaction())
                    {
                        var removedInactive = RemoveInactiveScratches(_current);

                        var removedInactiveRecycled = RemoveInactiveRecycledScratches();

                        if (_logger.IsDebugEnabled)
                        {
                            _logger.Debug(
                                $"Cleanup of {nameof(ScratchBufferPool)} removed: {removedInactive} inactive scratches and {removedInactiveRecycled} inactive from the recycle area");
                        }

                        _forTestingPurposes?.ActionToCallDuringCleanupRightAfterRemovingInactiveScratches?.Invoke();
                    }
                }
                catch (TimeoutException)
                {

                }
                catch (DiskFullException)
                {

                }
                finally
                {
                    exitPreventNewTransactions?.Dispose();
                }
            }
            finally
            {
                byteStringContext.Dispose();
            }
        }


        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff(this);
        }

        internal sealed class TestingStuff
        {
            private readonly ScratchBufferPool _env;

            internal Action ActionToCallDuringCleanupRightAfterRemovingInactiveScratches;

            internal Action ActionToCallDuringRemovalsOfInactiveScratchesRightAfterDisposingScratch;

            internal Action ActionToCallDuringRemovalsOfRecycledScratchesRightAfterDisposingScratches;

            public TestingStuff(ScratchBufferPool env)
            {
                _env = env;
            }

            internal IDisposable CallDuringCleanupRightAfterRemovingInactiveScratches(Action action)
            {
                ActionToCallDuringCleanupRightAfterRemovingInactiveScratches = action;

                return new DisposableAction(() => ActionToCallDuringCleanupRightAfterRemovingInactiveScratches = null);
            }

            internal IDisposable CallDuringRemovalsOfInactiveScratchesRightAfterDisposingScratch(Action action)
            {
                ActionToCallDuringRemovalsOfInactiveScratchesRightAfterDisposingScratch = action;

                return new DisposableAction(() => ActionToCallDuringRemovalsOfInactiveScratchesRightAfterDisposingScratch = null);
            }

            internal IDisposable CallDuringRemovalsOfRecycledScratchesRightAfterDisposingScratch(Action action)
            {
                ActionToCallDuringRemovalsOfRecycledScratchesRightAfterDisposingScratches = action;

                return new DisposableAction(() => ActionToCallDuringRemovalsOfRecycledScratchesRightAfterDisposingScratches = null);
            }
        }

        private int RemoveInactiveRecycledScratches()
        {
            if (_recycleArea.Count == 0)
                return 0;

            var removed = 0;

            var scratchNode = _recycleArea.First;
            while (scratchNode != null)
            {
                var next = scratchNode.Next;

                var recycledScratch = scratchNode.Value;
                if (recycledScratch.File.HasActivelyUsedBytes(_env.PossibleOldestReadTransaction(null)) == false)
                {
                    if (_scratchBuffers.TryRemove(recycledScratch.Number, out var removedFile))
                    {
                        _scratchBuffersByPager.TryRemove(removedFile.File.Pager, out _);
                    }
                    recycledScratch.File.Dispose();
                    _scratchSpaceMonitor.Decrease(recycledScratch.File.NumberOfAllocatedPages * Constants.Storage.PageSize);

                    _recycleArea.Remove(scratchNode);

                    removed++;
                }

                scratchNode = next;
            }

            return removed;
        }

        public ScratchBufferPoolInfo InfoForDebug(long oldestActiveTransaction)
        {
            var currentFile = _current.File;
            var scratchBufferPoolInfo = new ScratchBufferPoolInfo
            {
                OldestActiveTransaction = oldestActiveTransaction,
                NumberOfScratchFiles = _scratchBuffers.Count,
                CurrentFileNumber = currentFile.Number,
                CurrentFileSizeInMB = currentFile.Size / 1024L / 1024L,
                PerScratchFileSizeLimitInMB = _options.MaxScratchBufferSize / 1024L / 1024L,
                CurrentUtcTime = DateTime.UtcNow
            };

            foreach (var scratchBufferItem in _scratchBuffers.Values.OrderBy(x => x.Number))
            {
                var current = _current;
                var scratchFileUsage = new ScratchFileUsage
                {
                    Name = StorageEnvironmentOptions.ScratchBufferName(scratchBufferItem.File.Number),
                    SizeInKB = scratchBufferItem.File.Size / 1024,
                    NumberOfAllocations = scratchBufferItem.File.NumberOfAllocations,
                    AllocatedPagesCount = scratchBufferItem.File.AllocatedPagesCount,
                    CanBeDeleted = scratchBufferItem != current && scratchBufferItem.File.HasActivelyUsedBytes(oldestActiveTransaction) == false,
                    TxIdAfterWhichLatestFreePagesBecomeAvailable = scratchBufferItem.File.TxIdAfterWhichLatestFreePagesBecomeAvailable,
                    IsInRecycleArea = _recycleArea.Contains(scratchBufferItem),
                    NumberOfResets = scratchBufferItem.File.DebugInfo.NumberOfResets,
                    LastResetTime = scratchBufferItem.File.DebugInfo.LastResetTime,
                    LastFreeTime = scratchBufferItem.File.DebugInfo.LastFreeTime
                };

                foreach (var freePage in scratchBufferItem.File.DebugInfo.GetMostAvailableFreePagesBySize())
                {
                    scratchFileUsage.MostAvailableFreePages.Add(new MostAvailableFreePagesBySize
                    {
                        Size = freePage.Key,
                        ValidAfterTransactionId = freePage.Value
                    });
                }

                foreach (var allocatedPage in scratchBufferItem.File.DebugInfo.GetFirst10AllocatedPages())
                {
                    scratchFileUsage.First10AllocatedPages.Add(new AllocatedPageInScratchBuffer()
                    {
                        NumberOfPages = allocatedPage.NumberOfPages,
                        PositionInScratchBuffer = allocatedPage.PositionInScratchBuffer,
                        ScratchFileNumber = allocatedPage.File.Number,
                    });
                }

                scratchBufferPoolInfo.ScratchFilesUsage.Add(scratchFileUsage);
            }

            return scratchBufferPoolInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsLowMemory()
        {
            if (_lowMemoryFlag.IsRaised())
                return true;

            return DateTime.UtcNow.Ticks - _lastLowMemoryEventTicks <= _lowMemoryIntervalTicks;
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            _lastLowMemoryEventTicks = DateTime.UtcNow.Ticks;
            _lowMemoryFlag.Raise();
        }

        public void LowMemoryOver()
        {
            _lowMemoryFlag.Lower();
        }

        public ScratchBufferFile GetScratchBufferFile(int number)
        {
            return _scratchBuffers.TryGetValue(number, out var item) ? item.File : null;
        }
        
        public ScratchBufferFile GetScratchBufferFile(Pager pager)
        {
            return _scratchBuffersByPager.TryGetValue(pager, out var item) ? item.File : null;
        }
    }
}
