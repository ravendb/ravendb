using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Data;
using Voron.Exceptions;
using Voron.Global;
using Voron.Util.Settings;

namespace Voron.Impl.Paging
{
    public abstract unsafe class AbstractPager : IDisposable, ILowMemoryHandler
    {
        private readonly StorageEnvironmentOptions _options;

        public static ConcurrentDictionary<string, uint> PhysicalDrivePerMountCache = new ConcurrentDictionary<string, uint>();

        protected int MinIncreaseSize => 16 * Constants.Size.Kilobyte;

        protected int MaxIncreaseSize => Constants.Size.Gigabyte;

        private long _increaseSize;
        private DateTime _lastIncrease;
        private readonly object _pagerStateModificationLocker = new object();
        public bool UsePageProtection { get; } = false;
        private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();

        public Action<PagerState> PagerStateChanged;

        public Func<long> AllocatedInBytesFunc;

        public long GetAllocatedInBytes()
        {
            return AllocatedInBytesFunc?.Invoke() ?? NumberOfAllocatedPages * Constants.Storage.PageSize;
        }

        public void SetPagerState(PagerState newState)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            lock (_pagerStateModificationLocker)
            {
                newState.AddRef();

                if (ShouldLockMemoryAtPagerLevel())
                {                    
                    // Note: This is handled differently in 32-bits.
                    // Locking/unlocking the memory is done separately for each mapping.
                    try
                    {
                        foreach (var info in newState.AllocationInfos)
                        {
                            if (info.Size == 0 || info.BaseAddress == null)
                                continue;

                            if (Sodium.sodium_mlock(info.BaseAddress, (UIntPtr)info.Size) != 0)
                            {
                                if (DoNotConsiderMemoryLockFailureAsCatastrophicError)
                                    continue; // okay, can skip this, then

                                var sumOfAllocations = Bits.NextPowerOf2(newState.AllocationInfos.Sum(x => x.Size) * 2);
                                if (TryHandleFailureToLockMemory(info.BaseAddress, info.Size, sumOfAllocations))
                                    break;
                            }
                        }
                    }
                    catch 
                    {
                        // need to restore the state to the way it was, so we'll dispose the pager state
                        newState.Release();
                        throw;
                    }
                }
                
                _debugInfo = GetSourceName();
                var oldState = _pagerState;
                _pagerState = newState;
                PagerStateChanged?.Invoke(newState);
                oldState?.Release();
            }
        }

        public virtual bool ShouldLockMemoryAtPagerLevel()
        {
            return LockMemory;
        }

        protected bool TryHandleFailureToLockMemory(byte* addressToLock, long sizeToLock, long sumOfAllocationsInBytes)
        {
            var currentProcess = Process.GetCurrentProcess();

            if (PlatformDetails.RunningOnPosix == false)
            {
                // From: https://msdn.microsoft.com/en-us/library/windows/desktop/ms686234(v=vs.85).aspx
                // "The maximum number of pages that a process can lock is equal to the number of pages in its minimum working set minus a small overhead"
                // let's increase the max size of memory we can lock by increasing the MinWorkingSet. On Windows, that is available for all users
                var nextSize = Bits.NextPowerOf2(currentProcess.MinWorkingSet.ToInt64() + sumOfAllocationsInBytes + sizeToLock);
                if (nextSize > int.MaxValue && IntPtr.Size == sizeof(int))
                {
                    nextSize = int.MaxValue;
                }
                
                // Minimum working set size must be less than or equal to the maximum working set size.
                // Let's increase the max as well.
                if (nextSize > (long)currentProcess.MaxWorkingSet)
                {
                    try
                    {
                        currentProcess.MaxWorkingSet = new IntPtr(nextSize);
                    }
                    catch (Exception e)
                    {
                        throw new InsufficientMemoryException($"Need to increase the min working set size from {(long)currentProcess.MinWorkingSet:#,#;;0} to {nextSize:#,#;;0} but the max working set size was too small: {(long)currentProcess.MaxWorkingSet:#,#;;0}. " +
                                                              $"Failed to increase the max working set size so we can lock {sizeToLock:#,#;;0} for {FileName}. With encrypted " +
                                                              "databases we lock some memory in order to avoid leaking secrets to disk. Treating this as a catastrophic error " +
                                                              "and aborting the current operation.", e);
                    }
                }

                try
                {
                    currentProcess.MinWorkingSet = new IntPtr(nextSize);
                }
                catch (Exception e)
                {
                    throw new InsufficientMemoryException($"Failed to increase the min working set size so we can lock {sizeToLock:#,#;;0} for {FileName}. With encrypted " +
                                                          "databases we lock some memory in order to avoid leaking secrets to disk. Treating this as a catastrophic error " +
                                                          "and aborting the current operation.", e);
                }

                // now we can try again, after we raised the limit, we only do so once, though
                if (Sodium.sodium_mlock(addressToLock, (UIntPtr)sizeToLock) == 0)
                    return false;
            }

            var msg =
                $"Unable to lock memory for {FileName} with size {sizeToLock:#,#;;0}), with encrypted databases we lock some memory in order to avoid leaking secrets to disk. Treating this as a catastrophic error and aborting the current operation.{Environment.NewLine}";
            if (PlatformDetails.RunningOnPosix)
            {
                msg +=
                    $"The admin may configure higher limits using: 'sudo prlimit --pid {currentProcess.Id} --memlock={sumOfAllocationsInBytes}' to increase the limit. (It's recommended to do that as part of the startup script){Environment.NewLine}";
            }
            else
            {
                msg +=
                    $"Already tried to raise the the process min working set to {currentProcess.MinWorkingSet.ToInt64():#,#;;0} but still got a failure.{Environment.NewLine}";
            }

            msg += "This behavior is controlled by the 'Security.DoNotConsiderMemoryLockFailureAsCatastrophicError' setting (expert only, modifications of this setting is not recommended).";

            throw new InsufficientMemoryException(msg);
        }

        internal PagerState GetPagerStateAndAddRefAtomically()
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            lock (_pagerStateModificationLocker)
            {
                if (_pagerState == null)
                    return null;
                _pagerState.AddRef();
                return _pagerState;
            }
        }

        public PagerState PagerState
        {
            get
            {
                if (DisposeOnceRunner.Disposed)
                    ThrowAlreadyDisposedException();
                return _pagerState;
            }
        }

        private string _debugInfo;

        public string DebugInfo => _debugInfo;

        public const int PageMaxSpace = Constants.Storage.PageSize - Constants.Tree.PageHeaderSize;

        public VoronPathSetting FileName { get; protected set; }

        protected AbstractPager(StorageEnvironmentOptions options, bool usePageProtection = false)
        {
            DisposeOnceRunner = new DisposeOnce<SingleAttempt>(() =>
            {
                if (FileName?.FullPath != null)
                    _options?.IoMetrics?.FileClosed(FileName.FullPath);

                if (_pagerState != null)
                {
                    _pagerState.Release();
                    _pagerState = null;
                }

                if (FileName?.FullPath != null)
                    NativeMemory.UnregisterFileMapping(FileName.FullPath);

                DisposeInternal();
            });

            _options = options;
            UsePageProtection = usePageProtection;
            Debug.Assert((Constants.Storage.PageSize - Constants.Tree.PageHeaderSize) / Constants.Tree.MinKeysInPage >= 1024);


            NodeMaxSize = PageMaxSpace / 2 - 1;

            // MaxNodeSize is usually persisted as an unsigned short. Therefore, we must ensure it is not possible to have an overflow.
            Debug.Assert(NodeMaxSize < ushort.MaxValue);

            _increaseSize = MinIncreaseSize;
            PageMinSpace = (int)(PageMaxSpace * 0.33);

            SetPagerState(new PagerState(this));

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public StorageEnvironmentOptions Options => _options;

        public int PageMinSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        public bool DeleteOnClose
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set;
        }

        public int NodeMaxSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        public const int RequiredSpaceForNewNode = Constants.Tree.NodeHeaderSize + Constants.Tree.NodeOffsetSize;

        protected PagerState _pagerState;

        public virtual long NumberOfAllocatedPages { get; protected set; }
        public abstract long TotalAllocationSize { get; }

        protected abstract string GetSourceName();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* AcquirePagePointerInternal(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState)
        {
            if (DisposeOnceRunner.Disposed)
                goto AlreadyDisposed;

            if (pageNumber > NumberOfAllocatedPages || pageNumber < 0)
                goto InvalidPageNumber;

            var state = pagerState ?? _pagerState;

            tx?.EnsurePagerStateReference(state);

            return state.MapBase + pageNumber * Constants.Storage.PageSize;

            AlreadyDisposed: ThrowAlreadyDisposedException();
            InvalidPageNumber: ThrowOnInvalidPageNumber(pageNumber);
            return null; // Will never happen. 
        }

        public virtual byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            return AcquirePagePointerInternal(tx, pageNumber, pagerState);
        }

        public virtual byte* AcquirePagePointerForNewPage(IPagerLevelTransactionState tx, long pageNumber, int numberOfPages, PagerState pagerState = null)
        {
            return AcquirePagePointerInternal(tx, pageNumber, pagerState);
        }

        public virtual void BreakLargeAllocationToSeparatePages(IPagerLevelTransactionState tx, long pageNumber)
        {
            // This method is implemented only in Crypto Pager
        }

        public abstract void Sync(long totalUnsynced);
        
        public PagerState EnsureContinuous(long requestedPageNumber, int numberOfPages)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            if (requestedPageNumber + numberOfPages <= NumberOfAllocatedPages)
                return null;

            // this ensure that if we want to get a range that is more than the current expansion
            // we will increase as much as needed in one shot
            var minRequested = (requestedPageNumber + numberOfPages) * Constants.Storage.PageSize;
            var allocationSize = Math.Max(NumberOfAllocatedPages * Constants.Storage.PageSize, Constants.Storage.PageSize);
            while (minRequested > allocationSize)
            {
                allocationSize = GetNewLength(allocationSize, minRequested);
            }

            return AllocateMorePages(allocationSize);
        }

        [Conditional("VALIDATE")]
        internal virtual void ProtectPageRange(byte* start, ulong size, bool force = false)
        {
            // This method is currently implemented only in WindowsMemoryMapPager and POSIX
        }

        [Conditional("VALIDATE")]
        internal virtual void UnprotectPageRange(byte* start, ulong size, bool force = false)
        {
            // This method is currently implemented only in WindowsMemoryMapPager and POSIX
        }

        public uint UniquePhysicalDriveId;
        protected readonly DisposeOnce<SingleAttempt> DisposeOnceRunner;
        public bool Disposed => DisposeOnceRunner.Disposed;

        /// <summary>
        /// This determine whatever we'll attempt to lock the memory
        /// so it will not go to the swap / core dumps
        /// </summary>
        public bool LockMemory;
        
        /// <summary>
        /// Control whatever we should treat memory lock errors as catastrophic errors
        /// or not. By default, we consider them catastrophic and fail immediately to
        /// avoid leaking any data. 
        /// </summary>
        public bool DoNotConsiderMemoryLockFailureAsCatastrophicError;

        protected abstract void DisposeInternal();

        public virtual void Dispose()
        {
            DisposeOnceRunner.Dispose();
        }

        ~AbstractPager()
        {
            DisposeOnceRunner.Dispose();
        }

        protected internal abstract PagerState AllocateMorePages(long newLength);

        private long GetNewLength(long current, long minRequested)
        {
            DateTime now = DateTime.UtcNow;
            if (_lastIncrease == DateTime.MinValue)
            {
                _lastIncrease = now;
                return MinIncreaseSize;
            }

            if (_lowMemoryFlag)
            {
                _lastIncrease = now;
                // cannot return less than the minRequested
                return Bits.NextPowerOf2(minRequested);
            }

            TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
            _lastIncrease = now;
            if (timeSinceLastIncrease.TotalMinutes < 3)
            {
                _increaseSize = Math.Min(_increaseSize * 2, MaxIncreaseSize);
            }
            else if (timeSinceLastIncrease.TotalMinutes > 15)
            {
                _increaseSize = Math.Max(MinIncreaseSize, _increaseSize / 2);
            }

            // At any rate, we won't do an increase by over 50% of current size, to prevent huge empty spaces
            // 
            // The reasoning behind this is that we want to make sure that we increase in size very slowly at first
            // because users tend to be sensitive to a lot of "wasted" space. 
            // We also consider the fact that small increases in small files would probably result in cheaper costs, and as
            // the file size increases, we will reserve more & more from the OS.
            // This also avoids "I added 300 records and the file size is 64MB" problems that occur when we are too
            // eager to reserve space
            var actualIncrease = Math.Min(_increaseSize, current / 2);

            // we then want to get the next power of two number, to get pretty file size
            var totalSize = current + actualIncrease;
            if (totalSize < 512 * 1024 * 1024L)
                return Bits.NextPowerOf2(totalSize);

            // if it is over 0.5 GB, then we grow at 1 GB intervals
            var remainder = totalSize%Constants.Size.Gigabyte;
            if (remainder == 0)
                return totalSize;

            // above 0.5GB we need to round to the next GB number
            return totalSize + Constants.Size.Gigabyte - remainder;
        }


        public abstract override string ToString();


        public static void ThrowAlreadyDisposedException()
        {
            // this is a separate method because we don't want to have an exception throwing in the hot path
            throw new ObjectDisposedException("The pager is already disposed");
        }


        protected void ThrowOnInvalidPageNumber(long pageNumber)
        {
            // this is a separate method because we don't want to have an exception throwing in the hot path

            VoronUnrecoverableErrorException.Raise(_options,
                "The page " + pageNumber + " was not allocated, allocated pages: " + NumberOfAllocatedPages + " in " +
                GetSourceName());
        }

        public virtual void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            if (LockMemory == false) 
                return;
            
            // intentionally skipping verification of the result, nothing that we
            // can do about this here, and we are going to free the memory anyway
            // that at any rate, we don't care about the memory zeroing, since
            // we are already zeroing the memory ourselves (the pager is likely
            // to be part of a long term held instance).
            Sodium.sodium_munlock(baseAddress, (UIntPtr)size);
        }

        // NodeMaxSize - RequiredSpaceForNewNode for 4Kb page is 2038, so we drop this by a bit
        public const int MaxKeySize = 2038 - RequiredSpaceForNewNode;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsKeySizeValid(int keySize)
        {
            if (keySize > MaxKeySize)
                return false;

            return true;
        }

        public abstract void TryPrefetchingWholeFile();
        public abstract void MaybePrefetchMemory(List<long> pagesToPrefetch);

        public virtual bool EnsureMapped(IPagerLevelTransactionState tx, long page, int numberOfPages)
        {
            // nothing to do
            return false;
        }

        public abstract int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState);

        protected int CopyPageImpl(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            var src = AcquirePagePointer(null, p, pagerState);
            var pageHeader = (PageHeader*)src;
            int numberOfPages = 1;
            if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
            {
                numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            }
            const int adjustPageSize = (Constants.Storage.PageSize)/(4*Constants.Size.Kilobyte);
            destwI4KbBatchWrites.Write(pageHeader->PageNumber * (long)adjustPageSize, numberOfPages * adjustPageSize, src);

            return numberOfPages;
        }

        public virtual I4KbBatchWrites BatchWriter()
        {
            return new Simple4KbBatchWrites(this);
        }

        public virtual byte* AcquireRawPagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            return AcquirePagePointer(tx, pageNumber, pagerState);
        }

        public void LowMemory()
        {
            // We could check for nested calls to LowMemory here, but we 
            // probably don't want to error because of it.
            _lowMemoryFlag.Raise();
        }

        public void LowMemoryOver()
        {
            _lowMemoryFlag.Lower();
        }
    }

    public interface I4KbBatchWrites : IDisposable
    {
        unsafe void Write(long posBy4Kbs, int numberOf4Kbs, byte* source);
    }

    public unsafe class Simple4KbBatchWrites : I4KbBatchWrites
    {
        private readonly AbstractPager _abstractPager;
        private PagerState _pagerState;

        public Simple4KbBatchWrites(AbstractPager abstractPager)
        {
            _abstractPager = abstractPager;
            _pagerState = _abstractPager.GetPagerStateAndAddRefAtomically();
        }

        public void Write(long posBy4Kbs, int numberOf4Kbs, byte* source)
        {
            const int pageSizeTo4KbRatio = (Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte));
            var pageNumber = posBy4Kbs / pageSizeTo4KbRatio;
            var offsetBy4Kb = posBy4Kbs % pageSizeTo4KbRatio;
            var numberOfPages = numberOf4Kbs / pageSizeTo4KbRatio;
            if (numberOf4Kbs % pageSizeTo4KbRatio != 0)
                numberOfPages++;

            var newPagerState = _abstractPager.EnsureContinuous(pageNumber, numberOfPages);
            if (newPagerState != null)
            {
                _pagerState.Release();
                newPagerState.AddRef();
                _pagerState = newPagerState;
            }

            var toWrite = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
            byte* destination = _abstractPager.AcquirePagePointer(null, pageNumber, _pagerState)
                                + (offsetBy4Kb * 4 * Constants.Size.Kilobyte);

            _abstractPager.UnprotectPageRange(destination, (ulong)toWrite);

            Memory.Copy(destination, source, toWrite);

            _abstractPager.ProtectPageRange(destination, (ulong)toWrite);
        }


        public void Dispose()
        {
            _pagerState.Release();
        }
    }
}

