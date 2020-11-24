using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Util;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Compression
{
    public unsafe class DecompressionBuffersPool : IDisposable
    {
        private readonly object _expandPoolLock = new object();
        private readonly object _decompressionPagerLock = new object();

        private readonly StorageEnvironmentOptions _options;

        private ConcurrentQueue<DecompressionBuffer>[] _pool;
        private long _decompressionPagerCounter;
        private long _lastUsedPage;
        private AbstractPager _compressionPager;
        private bool _initialized;

        private long _currentlyUsedBytes;

        private ImmutableAppendOnlyList<AbstractPager> _oldPagers;
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
                    _compressionPager.Dispose();

                    _scratchSpaceMonitor.Decrease(_compressionPager.NumberOfAllocatedPages * Constants.Storage.PageSize);
                }

                foreach (var pager in _oldPagers)
                {
                    if (pager.Disposed == false)
                    {
                        pager.Dispose();

                        _scratchSpaceMonitor.Decrease(pager.NumberOfAllocatedPages * Constants.Storage.PageSize);
                    }
                }
            });
        }

        private AbstractPager CreateDecompressionPager(long initialSize)
        {
            var pager = _options.CreateTemporaryBufferPager($"decompression.{_decompressionPagerCounter++:D10}.buffers", initialSize);

            _scratchSpaceMonitor.Increase(pager.NumberOfAllocatedPages * Constants.Storage.PageSize);

            return pager;
        }

        public DecompressedLeafPage GetPage(LowLevelTransaction tx, int pageSize, DecompressionUsage usage, TreePage original)
        {
            GetTemporaryPage(tx, pageSize, out var tempPage);

            var treePage = tempPage.GetTempPage();

            return new DecompressedLeafPage(treePage.Base, treePage.PageSize, usage, original, tempPage);
        }

        public IDisposable GetTemporaryPage(LowLevelTransaction tx, int pageSize, out TemporaryPage tmp)
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
                if (buffer.CanReuse == false)
                    continue;

                try
                {
                    buffer.EnsureValidPointer(tx);
                    tmp = buffer.TempPage;
                    break;
                }
                catch (ObjectDisposedException)
                {
                    // we could dispose the pager during the cleanup
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
                        var numberOfPagesBeforeAllocate = _compressionPager.NumberOfAllocatedPages;

                        _compressionPager.EnsureContinuous(_lastUsedPage, allocationInPages);

                        if (_compressionPager.NumberOfAllocatedPages > numberOfPagesBeforeAllocate)
                            _scratchSpaceMonitor.Increase((_compressionPager.NumberOfAllocatedPages - numberOfPagesBeforeAllocate) * Constants.Storage.PageSize);
                    }
                    catch (InsufficientMemoryException)
                    {
                        // RavenDB-10830: failed to lock memory of temp buffers in encrypted db, let's create new file with initial size

                        CreateNewBuffersPager(DecompressedPagesCache.Size * Constants.Compression.MaxPageSize);
                        throw;
                    }

                    buffer = new DecompressionBuffer(_compressionPager, _lastUsedPage, pageSize, this, index, tx);

                    _lastUsedPage += allocationInPages;

                    void CreateNewBuffersPager(long size)
                    {
                        _oldPagers = _oldPagers.Append(_compressionPager);
                        _compressionPager = CreateDecompressionPager(size);
                        _lastUsedPage = 0;
                    }
                }

                tmp = buffer.TempPage;
            }

            Interlocked.Add(ref _currentlyUsedBytes, pageSize);

            return tmp.ReturnTemporaryPageToPool;
        }

        private static void ThrowPageSizeTooBig(int pageSize)
        {
            throw new ArgumentException($"Max page size is {Constants.Compression.MaxPageSize} while you requested {pageSize} bytes");
        }

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
                _compressionPager = CreateDecompressionPager(DecompressedPagesCache.Size * Constants.Compression.MaxPageSize);
                _oldPagers = ImmutableAppendOnlyList<AbstractPager>.Empty;
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

        public void Cleanup()
        {
            if (_initialized == false)
                return;

            if (_oldPagers.Count == 0)
                return;

            var necessaryPages = Interlocked.Read(ref _currentlyUsedBytes) / Constants.Storage.PageSize;

            var availablePages = _compressionPager.NumberOfAllocatedPages;

            var pagers = _oldPagers;

            for (var i = pagers.Count - 1; i >= 0; i--)
            {
                var old = pagers[i];

                if (availablePages >= necessaryPages)
                {
                    old.Dispose();
                    _scratchSpaceMonitor.Decrease(old.NumberOfAllocatedPages * Constants.Storage.PageSize);

                    continue;
                }

                // PERF: We dont care about the pager data content anymore. So we can discard the whole context to
                //       clean up the modified bit.
                old.DiscardWholeFile();
                availablePages += old.NumberOfAllocatedPages;
            }

            _oldPagers = _oldPagers.RemoveWhile(x => x.Disposed);
        }

        private class DecompressionBuffer : IDisposable
        {
            private readonly AbstractPager _pager;
            private readonly long _position;
            private readonly int _size;
            private readonly DecompressionBuffersPool _pool;
            private readonly int _index;

            public DecompressionBuffer(AbstractPager pager, long position, int size, DecompressionBuffersPool pool, int index, LowLevelTransaction tx)
            {
                _pager = pager;
                _position = position;
                _size = size;
                _pool = pool;
                _index = index;
                _pager.EnsureMapped(tx, _position, _size / Constants.Storage.PageSize);
                var ptr = _pager.AcquirePagePointer(tx, position);

                TempPage = new TemporaryPage(ptr, size) { ReturnTemporaryPageToPool = this };
            }

            public readonly TemporaryPage TempPage;

            public void EnsureValidPointer(LowLevelTransaction tx)
            {
                _pager.EnsureMapped(tx, _position, _size / Constants.Storage.PageSize);
                var p = _pager.AcquirePagePointer(tx, _position);

                TempPage.SetPointer(p);
            }

            public bool CanReuse => _pager.Disposed == false;

            public void Dispose()
            {
                if (_pager.Options.Encryption.IsEnabled)
                    Sodium.sodium_memzero(TempPage.TempPagePointer, (UIntPtr)TempPage.PageSize);

                // return it to the pool
                _pool._pool[_index].Enqueue(this);

                Interlocked.Add(ref _pool._currentlyUsedBytes, -_size);
            }
        }
    }
}
