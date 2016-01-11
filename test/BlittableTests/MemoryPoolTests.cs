using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Json;
using Xunit;

namespace BlittableTests
{
    public unsafe class MemoryPoolTests
    {
        [Fact]
        public void SerialAllocationAndRelease()
        {
            using (var pool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
            {
                var allocatedMemory = new List<Tuple<int, ulong>>();
                for (var i = 0; i < 1000; i++)
                {
                    int curSize = 0;
                    ulong curAddress = 0;
                    curAddress = (ulong) pool.GetMemory(i, out curSize);
                    allocatedMemory.Add(Tuple.Create(curSize,curAddress));
                }
                foreach (var tuple in allocatedMemory)
                {
                    pool.ReturnMemory((byte*)tuple.Item2);
                }
            }
        }

        [Fact]
        public void ParallelAllocationAndReleaseSeperately()
        {
            using (var pool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
            {
                var allocatedMemory = new Sparrow.Collections.ConcurrentSet<Tuple<int, ulong>>();
                Parallel.For(0, 1000, x =>
                {
                    for (var i = 0; i < 1000; i++)
                    {
                        int curSize = 0;
                        ulong curAddress = 0;
                        curAddress = (ulong) pool.GetMemory(i, out curSize);
                        allocatedMemory.Add(Tuple.Create(curSize, curAddress));
                    }
                });

                Parallel.ForEach(allocatedMemory, tuple =>
                {
                    pool.ReturnMemory((byte*) tuple.Item2);
                });
            }
        }

        [Fact]
        public void ParallelSerialAllocationAndRelease()
        {
            using (var pool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
            {
                var allocatedMemory = new BlockingCollection<Tuple<int, ulong>>();
                Task.Run(() =>
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        int curSize = 0;
                        ulong curAddress = 0;
                        curAddress = (ulong)pool.GetMemory(i, out curSize);
                        allocatedMemory.Add(Tuple.Create(curSize, curAddress));
                    }
                    allocatedMemory.CompleteAdding();
                });
                
                while (allocatedMemory.IsCompleted == false)
                {
                    Tuple<int, ulong> tuple;
                    if (allocatedMemory.TryTake(out tuple, 100))
                        pool.ReturnMemory((byte*) tuple.Item2);
                }
            }
        }
    }
}
