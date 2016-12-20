using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

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

        public DecompressionBuffersPool(StorageEnvironmentOptions options)
        {
            _options = options;
        }

        public AbstractPager CreateDecompressionPager(long initialSize)
        {
            return _options.CreateScratchPager($"decompression.{_decompressionPagerCounter++:D10}.buffers", initialSize); // TODO arek - allow to create multiple files, handle cleanup
        }

        public DecompressedLeafPage GetPage(LowLevelTransaction tx, int pageSize, DecompressionUsage usage, TreePage original)
        {
            TemporaryPage tempPage;
            GetTemporaryPage(tx, pageSize, out tempPage);

            var treePage = tempPage.GetTempPage();

            return new DecompressedLeafPage(treePage.Base, treePage.PageSize, usage, original, tempPage);
        }

        public IDisposable GetTemporaryPage(LowLevelTransaction tx, int pageSize, out TemporaryPage tmp)
        {
            if (pageSize < _options.PageSize)
                ThrowInvalidPageSize(pageSize);

            if (pageSize > Constants.Storage.MaxPageSize)
                ThrowPageSizeTooBig(pageSize);

            Debug.Assert(pageSize == Bits.NextPowerOf2(pageSize));

            EnsureInitialized();

            var index = GetTempPagesPoolIndex(pageSize);

            if (_pool.Length <= index)
            {
                lock (_expandPoolLock)
                {
                    if (_pool.Length <= index) // someone could get the lock and add it meanwhile
                    {
                        var oldSize = _pool.Length;

                        Array.Resize(ref _pool, index + 1);

                        for (var i = oldSize; i < _pool.Length; i++)
                        {
                            _pool[i] = new ConcurrentQueue<DecompressionBuffer>();
                        }
                    }
                }
            }

            DecompressionBuffer buffer;

            if (_pool[index].Count > 0 && _pool[index].TryDequeue(out buffer))
            {
                buffer.EnsureValidPointer(_compressionPager.AcquirePagePointer(tx, buffer.Position));

                tmp = buffer.TempPage;
            }
            else
            {
                var allocationInPages = pageSize / _options.PageSize;

                lock (_decompressionPagerLock) // once we fill up the pool we won't be allocating additional pages frequently
                {
                    _compressionPager.EnsureContinuous(_lastUsedPage, allocationInPages);

                    buffer = new DecompressionBuffer(_compressionPager.AcquirePagePointer(tx, _lastUsedPage), pageSize, _lastUsedPage, this, index);

                    _lastUsedPage += allocationInPages;
                }

                tmp = buffer.TempPage;
            }
            
            return tmp.ReturnTemporaryPageToPool;
        }

        private static void ThrowPageSizeTooBig(int pageSize)
        {
            throw new ArgumentException($"Max page size is {Constants.Storage.MaxPageSize} while you requested {pageSize} bytes");
        }

        private void ThrowInvalidPageSize(int pageSize)
        {
            throw new ArgumentException(
                $"Page cannot be smaller than {_options.PageSize} bytes while {pageSize} bytes were requested.");
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
                _compressionPager = CreateDecompressionPager(DecompressedPagesCache.Size * Constants.Storage.MaxPageSize);
                _initialized = true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetTempPagesPoolIndex(int pageSize)
        {
            if (pageSize == _options.PageSize)
                return 0;

            var index = 0;

            while (pageSize > _options.PageSize)
            {
                pageSize >>= 1;
                index++;
            }
            return index;
        }
        public void Dispose()
        {
            _compressionPager?.Dispose();
        }

        private class DecompressionBuffer : IDisposable
        {
            public DecompressionBuffer(byte* ptr, int size, long position, DecompressionBuffersPool pool, int index)
            {
                _ptr = ptr;
                Position = position;
                _pool = pool;
                _index = index;
                TempPage = new TemporaryPage(ptr, size) { ReturnTemporaryPageToPool = this };
            }

            private readonly byte* _ptr;
            public readonly long Position;
            private readonly DecompressionBuffersPool _pool;
            private readonly int _index;

            public readonly TemporaryPage TempPage;

            public void EnsureValidPointer(byte* p)
            {
                if (_ptr == p)
                    return;

                TempPage.SetPointer(p);
            }

            public void Dispose()
            {
                // return it to the pool
                _pool._pool[_index].Enqueue(this);
            }
        }
    }
}