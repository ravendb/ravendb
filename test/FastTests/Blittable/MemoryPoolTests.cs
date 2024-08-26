using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public unsafe class MemoryPoolTests : NoDisposalNeeded
    {
        public MemoryPoolTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SerialAllocationAndRelease()
        {
            using (var pool = new UnmanagedBuffersPoolWithLowMemoryHandling(RavenLogManager.CreateNullLogger(), string.Empty))
            {
                var allocatedMemory = new List<AllocatedMemoryData>();
                for (var i = 0; i < 1000; i++)
                {
                    allocatedMemory.Add(pool.Allocate(i));
                }
                foreach (var data in allocatedMemory)
                {
                    pool.Return(data);
                }
            }
        }

        [Fact]
        public void ParallelAllocationAndReleaseSeperately()
        {
            using (var pool = new UnmanagedBuffersPoolWithLowMemoryHandling(RavenLogManager.CreateNullLogger(), string.Empty))
            {
                var allocatedMemory = new global::Sparrow.Collections.ConcurrentSet<AllocatedMemoryData>();
                Parallel.For(0, 100, RavenTestHelper.DefaultParallelOptions, x =>
                {
                    for (var i = 0; i < 10; i++)
                    {
                        allocatedMemory.Add(pool.Allocate(i));
                    }
                });

                Parallel.ForEach(allocatedMemory, RavenTestHelper.DefaultParallelOptions, item =>
                {
                    pool.Return(item);
                });
            }
        }

        [Fact]
        public void ParallelSerialAllocationAndRelease()
        {
            using (var pool = new UnmanagedBuffersPoolWithLowMemoryHandling(RavenLogManager.CreateNullLogger(), string.Empty))
            {
                var allocatedMemory = new BlockingCollection<AllocatedMemoryData>();
                Task.Run(() =>
                {
                    for (var i = 0; i < 100; i++)
                    {
                        allocatedMemory.Add(pool.Allocate(i));
                    }
                    allocatedMemory.CompleteAdding();
                });

                while (allocatedMemory.IsCompleted == false)
                {
                    AllocatedMemoryData tuple;
                    if (allocatedMemory.TryTake(out tuple, 100))
                        pool.Return(tuple);
                }
            }
        }
    }
}
