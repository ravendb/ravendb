using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Compression
{
    public class DecompressedPagesPool : IDisposable
    {
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        private readonly StorageEnvironmentOptions _options;

        private ConcurrentQueue<TemporaryPage>[] _pool = { new ConcurrentQueue<TemporaryPage>() };

        public DecompressedPagesPool(StorageEnvironmentOptions options)
        {
            _options = options;
        }

        public IDisposable GetTemporaryPage(LowLevelTransaction tx, int pageSize, out TemporaryPage tmp)
        {
            if (pageSize < _options.PageSize)
                throw new ArgumentException($"Page cannot be smaller than {_options.PageSize} bytes while {pageSize} bytes were requested.");

            Debug.Assert(pageSize == Bits.NextPowerOf2(pageSize));

            var index = GetTempPagesPoolIndex(pageSize);

            if (_pool.Length <= index)
            {
                _lock.EnterWriteLock();
                try
                {
                    if (_pool.Length <= index) // someone could add it meanwhile
                    {
                        var oldSize = _pool.Length;

                        Array.Resize(ref _pool, index + 1);

                        for (var i = oldSize; i < _pool.Length; i++)
                        {
                            _pool[i] = new ConcurrentQueue<TemporaryPage>();
                        }
                    }
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }

            if (_pool[index].Count > 0 && _pool[index].TryDequeue(out tmp))
                return tmp.ReturnTemporaryPageToPool;

            tmp = new TemporaryPage(_options, pageSize);
            try
            {
                return tmp.ReturnTemporaryPageToPool = new ReturnDecompressedPageToPool(this, tmp);
            }
            catch (Exception)
            {
                tmp.Dispose();
                throw;
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
            _lock.EnterReadLock();

            try
            {
                foreach (var items in _pool)
                {
                    foreach (var page in items)
                    {
                        page.Dispose();
                    }
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        private class ReturnDecompressedPageToPool : IDisposable
        {
            private readonly TemporaryPage _tmp;
            private readonly DecompressedPagesPool _pool;

            public ReturnDecompressedPageToPool(DecompressedPagesPool pool, TemporaryPage tmp)
            {
                _tmp = tmp;
                _pool = pool;
            }

            public void Dispose()
            {
                try
                {
                    var index = _pool.GetTempPagesPoolIndex(_tmp.PageSize);

                    _pool._lock.EnterReadLock();
                    try
                    {
                        _pool._pool[index].Enqueue(_tmp);
                    }
                    finally
                    {
                        _pool._lock.ExitReadLock();
                    }
                }
                catch (Exception)
                {
                    _tmp.Dispose();
                    throw;
                }
            }
        }
    }
}