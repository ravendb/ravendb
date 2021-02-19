using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public unsafe class EncryptionBufferPool : NoDisposalNeeded
    {
        public EncryptionBufferPool(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void dont_pool_buffers_larger_than_8Mb()
        {
            var encryptionBuffersPool = new EncryptionBuffersPool();
            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            while (i < new Size(64, SizeUnit.Megabytes).GetDoubleValue(SizeUnit.Bytes))
            {
                var ptr = encryptionBuffersPool.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var maxSize = new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
            var totalSize = 0L;
            foreach (var o in toFree)
            {
                if (o.Item2 <= maxSize)
                    totalSize += o.Item2;

                encryptionBuffersPool.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, encryptionBuffersPool.Generation);
            }

            var stats = encryptionBuffersPool.GetStats();
            Assert.Equal(totalSize, stats.TotalPoolSize);

            i = 1;
            foreach (var allocationInfo in stats.Details)
            {
                Assert.Equal(1, allocationInfo.NumberOfItems);
                Assert.Equal(i, allocationInfo.AllocationSize);
                i *= 2;
            }

            ClearMemory(encryptionBuffersPool);
        }

        [Theory]
        [InlineData(LowMemorySeverity.Low)]
        [InlineData(LowMemorySeverity.ExtremelyLow)]
        public void clear_all_buffers_from_current_generation_on_low_memory(LowMemorySeverity lowMemorySeverity)
        {
            var encryptionBuffersPool = new EncryptionBuffersPool();
            var generation = encryptionBuffersPool.Generation;
            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            while (i <= new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes))
            {
                var ptr = encryptionBuffersPool.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var stats = encryptionBuffersPool.GetStats();
            Assert.Equal(0, stats.TotalPoolSize);

            encryptionBuffersPool.LowMemory(lowMemorySeverity);
            stats = encryptionBuffersPool.GetStats();
            Assert.Equal(0, stats.TotalPoolSize);

            foreach (var o in toFree)
            {
                encryptionBuffersPool.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, generation);
            }

            stats = encryptionBuffersPool.GetStats();
            Assert.Equal(0, stats.TotalPoolSize);

            var size = 8 * 1024;
            var pointer = encryptionBuffersPool.Get(size, out _);
            encryptionBuffersPool.Return(pointer, size, NativeMemory.ThreadAllocations.Value, encryptionBuffersPool.Generation);

            // will cache the buffer
            stats = encryptionBuffersPool.GetStats();
            Assert.Equal(size, stats.TotalPoolSize);

            // will continue to cache the buffer
            encryptionBuffersPool.LowMemory(lowMemorySeverity);
            stats = encryptionBuffersPool.GetStats();
            Assert.Equal(size, stats.TotalPoolSize);

            encryptionBuffersPool.LowMemoryOver();
            ClearMemory(encryptionBuffersPool);
        }

        [Fact]
        public void clear_all_buffers_on_extremely_low_memory()
        {
            var encryptionBuffersPool = new EncryptionBuffersPool();
            var generation = encryptionBuffersPool.Generation;
            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            while (i <= new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes))
            {
                var ptr = encryptionBuffersPool.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var stats = encryptionBuffersPool.GetStats();
            Assert.Equal(0, stats.TotalPoolSize);

            foreach (var o in toFree)
            {
                encryptionBuffersPool.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, generation);
            }

            stats = encryptionBuffersPool.GetStats();
            var allocated = toFree.Sum(x => x.Item2);
            Assert.Equal(allocated, stats.TotalPoolSize);

            ClearMemory(encryptionBuffersPool);
        }

        [Fact]
        public void can_save_buffers_after_low_memory()
        {
            var encryptionBuffersPool = new EncryptionBuffersPool();
            encryptionBuffersPool.LowMemory(LowMemorySeverity.ExtremelyLow);
            encryptionBuffersPool.LowMemoryOver();

            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            var generation = encryptionBuffersPool.Generation;
            while (i <= new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes))
            {
                var ptr = encryptionBuffersPool.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var stats = encryptionBuffersPool.GetStats();
            Assert.Equal(0, stats.TotalPoolSize);

            foreach (var o in toFree)
            {
                encryptionBuffersPool.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, generation);
            }

            stats = encryptionBuffersPool.GetStats();
            var allocated = toFree.Sum(x => x.Item2);
            Assert.Equal(allocated, stats.TotalPoolSize);

            ClearMemory(encryptionBuffersPool);
        }

        [Fact]
        public void clear_buffers_only_when_in_extremely_low_memory()
        {
            var encryptionBuffersPool = new EncryptionBuffersPool();

            var ptr = encryptionBuffersPool.Get(1, out _);
            var stats = encryptionBuffersPool.GetStats();
            Assert.Equal(0, stats.TotalPoolSize);

            encryptionBuffersPool.Return(ptr, 1, NativeMemory.ThreadAllocations.Value, encryptionBuffersPool.Generation);
            stats = encryptionBuffersPool.GetStats();
            Assert.Equal(1, stats.TotalPoolSize);

            encryptionBuffersPool.LowMemory(LowMemorySeverity.Low);
            stats = encryptionBuffersPool.GetStats();
            Assert.Equal(1, stats.TotalPoolSize);

            ClearMemory(encryptionBuffersPool);
        }

        private static void ClearMemory(EncryptionBuffersPool encryptionBuffersPool)
        {
            encryptionBuffersPool.LowMemory(LowMemorySeverity.ExtremelyLow);
            var stats = encryptionBuffersPool.GetStats();
            Assert.Equal(0, stats.TotalPoolSize);
        }
    }
}
