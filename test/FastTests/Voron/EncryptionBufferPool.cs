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
            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            while (i < new Size(64, SizeUnit.Megabytes).GetDoubleValue(SizeUnit.Bytes))
            {
                var ptr = EncryptionBuffersPool.Instance.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var maxSize = new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
            var totalSize = 0L;
            foreach (var o in toFree)
            {
                if (o.Item2 <= maxSize)
                    totalSize += o.Item2;

                EncryptionBuffersPool.Instance.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, EncryptionBuffersPool.Instance.Generation);
            }

            var stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(totalSize, stats.TotalSize);

            i = 1;
            foreach (var allocationInfo in stats.Details)
            {
                Assert.Equal(1, allocationInfo.NumberOfItems);
                Assert.Equal(i, allocationInfo.AllocationSize);
                i *= 2;
            }
        }

        [Theory]
        [InlineData(LowMemorySeverity.Low)]
        [InlineData(LowMemorySeverity.ExtremelyLow)]
        public void clear_all_buffers_from_current_generation_on_low_memory(LowMemorySeverity lowMemorySeverity)
        {
            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            var generation = EncryptionBuffersPool.Instance.Generation;
            while (i <= new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes))
            {
                var ptr = EncryptionBuffersPool.Instance.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(0, stats.TotalSize);

            EncryptionBuffersPool.Instance.LowMemory(lowMemorySeverity);
            stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(0, stats.TotalSize);

            foreach (var o in toFree)
            {
                EncryptionBuffersPool.Instance.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, generation);
            }

            stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(0, stats.TotalSize);

            var size = 8 * 1024;
            var pointer = EncryptionBuffersPool.Instance.Get(size, out _);
            EncryptionBuffersPool.Instance.Return(pointer, size, NativeMemory.ThreadAllocations.Value, EncryptionBuffersPool.Instance.Generation);

            // will cache the buffer
            stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(size, stats.TotalSize);

            // will continue to cache the buffer
            EncryptionBuffersPool.Instance.LowMemory(lowMemorySeverity);
            stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(size, stats.TotalSize);

            // return to the original state
            EncryptionBuffersPool.Instance.LowMemoryOver();
        }

        [Fact]
        public void clear_all_buffers_on_extremely_low_memory()
        {
            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            var generation = EncryptionBuffersPool.Instance.Generation;
            while (i <= new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes))
            {
                var ptr = EncryptionBuffersPool.Instance.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(0, stats.TotalSize);

            foreach (var o in toFree)
            {
                EncryptionBuffersPool.Instance.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, generation);
            }

            stats = EncryptionBuffersPool.Instance.GetStats();
            var allocated = toFree.Sum(x => x.Item2);
            Assert.Equal(allocated, stats.TotalSize);

            EncryptionBuffersPool.Instance.LowMemory(LowMemorySeverity.ExtremelyLow);
            stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(0, stats.TotalSize);

            // return to the original state
            EncryptionBuffersPool.Instance.LowMemoryOver();
        }

        [Fact]
        public void can_save_buffers_after_low_memory()
        {
            EncryptionBuffersPool.Instance.LowMemory(LowMemorySeverity.ExtremelyLow);
            EncryptionBuffersPool.Instance.LowMemoryOver();

            var i = 1;
            var toFree = new List<(IntPtr, long)>();

            var generation = EncryptionBuffersPool.Instance.Generation;
            while (i <= new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes))
            {
                var ptr = EncryptionBuffersPool.Instance.Get(i, out _);
                toFree.Add(((IntPtr)ptr, i));

                i *= 2;
            }

            var stats = EncryptionBuffersPool.Instance.GetStats();
            Assert.Equal(0, stats.TotalSize);

            foreach (var o in toFree)
            {
                EncryptionBuffersPool.Instance.Return((byte*)o.Item1, o.Item2, NativeMemory.ThreadAllocations.Value, generation);
            }

            stats = EncryptionBuffersPool.Instance.GetStats();
            var allocated = toFree.Sum(x => x.Item2);
            Assert.Equal(allocated, stats.TotalSize);

            // clear all buffers and restore low memory state
            EncryptionBuffersPool.Instance.LowMemory(LowMemorySeverity.ExtremelyLow);
            EncryptionBuffersPool.Instance.LowMemoryOver();
        }
    }
}
