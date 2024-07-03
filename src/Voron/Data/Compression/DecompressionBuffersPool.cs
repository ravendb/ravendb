using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Platform;
using Sparrow.Threading;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Util;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Compression
{
    public sealed unsafe class DecompressionBuffersPool : IDisposable
    {
        private readonly object _expandPoolLock = new object();
        private readonly object _decompressionPagerLock = new object();

        private readonly StorageEnvironmentOptions _options;

        private ConcurrentQueue<DecompressionBuffer>[] _pool;
        private long _decompressionPagerCounter;
        private long _lastUsedPage;
        private PagerInfo _compressionPager;
        private bool _initialized;

        private long _currentlyUsedBytes;

        private ImmutableAppendOnlyList<PagerInfo> _oldPagers;
        private readonly long _maxNumberOfPagesInScratchBufferPool;

        internal int NumberOfScratchFiles => 1 + _oldPagers.Count;

        private readonly ScratchSpaceUsageMonitor _scratchSpaceMonitor;

        public DecompressionBuffersPool(StorageEnvironmentOptions options)
        {
            _options = options;
            _maxNumberOfPagesInScratchBufferPool = _options.MaxScratchBufferSize / Constants.Storage.PageSize;
            _scratchSpaceMonitor = options.ScratchSpaceUsage;

            _disposeOnceRunner = new DisposeOnce<SingleAttempt>(() =>
            {
                if (_initialized == false)
                    return;

                if (_compressionPager != null)
                {
                    _compressionPager.DisposePager();

                    _scratchSpaceMonitor.Decrease(_compressionPager.PagerState.NumberOfAllocatedPages * Constants.Storage.PageSize);
                }

                foreach (var pager in _oldPagers)
                {
                    if (pager.PagerState.Disposed == false)
                    {
                        pager.DisposePager();

                        _scratchSpaceMonitor.Decrease(pager.PagerState.NumberOfAllocatedPages * Constants.Storage.PageSize);
                    }
                }
            });
        }

        private (Pager2 Pager, Pager2.State State) CreateDecompressionPager(long initialSize)
        {
            var (pager, state) = _options.CreateTemporaryBufferPager(
                $"decompression.{_decompressionPagerCounter++:D10}{StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.BuffersFileExtension}", 
                initialSize, encrypted: _options.Encryption.IsEnabled);

            _scratchSpaceMonitor.Increase(state.NumberOfAllocatedPages * Constants.Storage.PageSize);

            return (pager, state);
        }

        public DecompressedLeafPage GetPage(LowLevelTransaction tx, int pageSize, DecompressionUsage usage, TreePage original)
        {
            var disposable = GetTemporaryPage(tx, pageSize, out var tempPage);
            TreePage.Initialize(tempPage, pageSize);
            return new DecompressedLeafPage(tempPage, pageSize, usage, original, disposable);
        }

        public IDisposable GetTemporaryPage(LowLevelTransaction tx, int pageSize, out byte* tmp)
        {
            if (pageSize < Constants.Storage.PageSize)
                ThrowInvalidPageSize(pageSize);

            if (pageSize > Constants.Compression.MaxPageSize)
                ThrowPageSizeTooBig(pageSize);

            Debug.Assert(pageSize == Bits.PowerOf2(pageSize));

            EnsureInitialized();

            var index = GetTempPagesPoolIndex(pageSize);

            if (_pool.Length <= index)
            {
                lock (_expandPoolLock)
                {
                    if (_pool.Length <= index) // someone could get the lock and add it meanwhile
                    {
                        var oldSize = _pool.Length;

                        var newPool = new ConcurrentQueue<DecompressionBuffer>[index + 1];
                        Array.Copy(_pool, newPool, _pool.Length);
                        for (var i = oldSize; i < newPool.Length; i++)
                        {
                            newPool[i] = new ConcurrentQueue<DecompressionBuffer>();
                        }
                        _pool = newPool;
                    }
                }
            }

            DecompressionBuffer buffer;

            var queue = _pool[index];

            tmp = null;

            while (queue.TryDequeue(out buffer))
            {
                if (buffer.PagerInfo.TryUse() == false)
                    continue;
                try
                {
                    buffer.EnsureValidPointer(tx);
                    tmp = buffer.Pointer;
                    break;
                }
                catch (Exception)
                {
                    // if we couldn't ensure valid pointer then we cannot proceed with that buffer
                }
            }

            if (tmp == null)
            {
                var allocationInPages = pageSize / Constants.Storage.PageSize;

                lock (_decompressionPagerLock) // once we fill up the pool we won't be allocating additional pages frequently
                {
                    if (_lastUsedPage + allocationInPages > _maxNumberOfPagesInScratchBufferPool)
                        CreateNewBuffersPager(_options.MaxScratchBufferSize);

                    try
                    {
                        var numberOfPagesBeforeAllocate = _compressionPager.PagerState.NumberOfAllocatedPages;

                        _compressionPager.Pager.EnsureContinuous(ref _compressionPager.PagerState, _lastUsedPage, allocationInPages);

                        if (_compressionPager.PagerState.NumberOfAllocatedPages > numberOfPagesBeforeAllocate)
                            _scratchSpaceMonitor.Increase((_compressionPager.PagerState.NumberOfAllocatedPages - numberOfPagesBeforeAllocate) * Constants.Storage.PageSize);
                    }
                    catch (InsufficientMemoryException)
                    {
                        // RavenDB-10830: failed to lock memory of temp buffers in encrypted db, let's create new file with initial size

                        CreateNewBuffersPager(DecompressedPagesCache.Size * Constants.Compression.MaxPageSize);
                        throw;
                    }

                    ObjectDisposedException.ThrowIf(_compressionPager.TryUse() == false, _compressionPager);

                    buffer = new DecompressionBuffer(_compressionPager, _lastUsedPage, pageSize, this, index, tx);

                    _lastUsedPage += allocationInPages;

                    void CreateNewBuffersPager(long size)
                    {
                        _oldPagers = _oldPagers.Append(_compressionPager);
                        var (pager, state) = CreateDecompressionPager(size);
                        _compressionPager = new PagerInfo(pager,state);
                        _lastUsedPage = 0;
                    }
                }

                tmp = buffer.Pointer;
            }

            Interlocked.Add(ref _currentlyUsedBytes, pageSize);

            return buffer;
        }

        [DoesNotReturn]
        private static void ThrowPageSizeTooBig(int pageSize)
        {
            throw new ArgumentException($"Max page size is {Constants.Compression.MaxPageSize} while you requested {pageSize} bytes");
        }

        [DoesNotReturn]
        private void ThrowInvalidPageSize(int pageSize)
        {
            throw new ArgumentException(
                $"Page cannot be smaller than {Constants.Storage.PageSize} bytes while {pageSize} bytes were requested.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureInitialized()
        {
            if (_initialized)
                return;

            lock (_decompressionPagerLock)
            {
                if (_initialized)
                    return;

                _pool = new[] { new ConcurrentQueue<DecompressionBuffer>() };
                var (pager, state) = CreateDecompressionPager(DecompressedPagesCache.Size * Constants.Compression.MaxPageSize);
                _compressionPager = new PagerInfo(pager, state);
                _oldPagers = ImmutableAppendOnlyList<PagerInfo>.Empty;
                _initialized = true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetTempPagesPoolIndex(int pageSize)
        {
            if (pageSize == Constants.Storage.PageSize)
                return 0;

            var index = 0;

            while (pageSize > Constants.Storage.PageSize)
            {
                pageSize >>= 1;
                index++;
            }
            return index;
        }

        private readonly DisposeOnce<SingleAttempt> _disposeOnceRunner;

        public void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        public int Cleanup()
        {
            var disposedCount = 0;

            if (_initialized == false)
                return disposedCount;

            if (_oldPagers.Count == 0)
                return disposedCount;

            var necessaryPages = Interlocked.Read(ref _currentlyUsedBytes) / Constants.Storage.PageSize;

            var availablePages = _compressionPager.PagerState.NumberOfAllocatedPages;

            var pagers = _oldPagers;

            for (var i = pagers.Count - 1; i >= 0; i--)
            {
                var old = pagers[i];

                if (old.TryTakeForDispose() == false)
                    continue;
                if (availablePages >= necessaryPages)
                {
                    old.DisposePager();
                    _scratchSpaceMonitor.Decrease(old.PagerState.NumberOfAllocatedPages * Constants.Storage.PageSize);
                    disposedCount++;
                    continue;
                }

                // PERF: We dont care about the pager data content anymore. So we can discard the whole context to
                //       clean up the modified bit.
                old.Pager.DiscardWholeFile(old.PagerState);
                availablePages += old.PagerState.NumberOfAllocatedPages;
            }

            _oldPagers = _oldPagers.RemoveWhile(x => x.PagerState.Disposed);
            return disposedCount;
        }

        private sealed class DecompressionBuffer : IDisposable
        {
            private bool _isDisposed;

            internal readonly PagerInfo PagerInfo;
            private readonly long _position;
            private readonly int _size;
            private readonly DecompressionBuffersPool _pool;
            private readonly int _index;
            public byte* Pointer;

            public DecompressionBuffer(PagerInfo pagerInfo, long position, int size, DecompressionBuffersPool pool, int index, LowLevelTransaction tx)
            {
                PagerInfo = pagerInfo;
                _position = position;
                _size = size;
                _pool = pool;
                _index = index;

                EnsureValidPointer(tx);
            }


            public void EnsureValidPointer(LowLevelTransaction tx)
            {
                PagerInfo.Pager.EnsureMapped(PagerInfo.PagerState, ref tx.PagerTransactionState, _position, _size / Constants.Storage.PageSize);
                Pointer = PagerInfo.Pager.AcquirePagePointer(PagerInfo.PagerState, ref tx.PagerTransactionState, _position);
                _isDisposed = false;
            }

            public void Dispose()
            {
                if (_isDisposed)
                    throw new InvalidOperationException("Double disposing is disallowed.");

                if (PagerInfo.PagerState.Disposed)
                    throw new InvalidOperationException("Pager has been disposed already.");

                if (PagerInfo.Pager.Options.Encryption.IsEnabled)
                    Sodium.sodium_memzero(Pointer, Constants.Storage.PageSize);

                // return it to the pool
                _pool._pool[_index].Enqueue(this);

                Interlocked.Add(ref _pool._currentlyUsedBytes, -_size);
                PagerInfo.Release();

                _isDisposed = true;
            }
        }

        private sealed class PagerInfo
        {
            internal readonly Pager2 Pager;
            internal Pager2.State PagerState;
            private long _numberOfUsages;

            public PagerInfo(Pager2 pager, Pager2.State pagerState)
            {
                Pager = pager;
                PagerState = pagerState;
            }

            public bool TryUse()
            {
                return Interlocked.Increment(ref _numberOfUsages) > 0;
            }

            public void Release()
            {
                Interlocked.Decrement(ref _numberOfUsages);
            }

            public bool TryTakeForDispose()
            {
                if (Interlocked.Read(ref _numberOfUsages) > 0)
                    return false;

                if (Interlocked.CompareExchange(ref _numberOfUsages, -(1000 * 1000), 0) != 0)
                    return false;

                return true;
            }

            public void DisposePager()
            {
                Pager.Dispose();
            }
        }
    }
}
