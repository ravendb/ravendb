using System;
using System.Collections.Generic;
using System.Text;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Global;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class AllocatorsStressTests : NoDisposalNeeded
    {
        private struct HashedPointer
        {
            public ulong Hash;
            public Pointer Pointer;
        }

        private const int InvalidSize = -1;

        private void StressTester<TAllocator, TOptions>(Allocator<TAllocator> allocator, int seed, bool supportsRenew = true, int fixedSize = InvalidSize)
            where TAllocator : struct, IAllocator<TAllocator, Pointer>
            where TOptions : struct, IAllocatorOptions
        {
            var rnd = new Random(seed);

            const int Allocate = 0;
            const int Deallocate = Allocate + 3;
            const int DeallocateLast = Deallocate + 1;
            const int TouchMemory = DeallocateLast + 1;
            const int CheckMemory = TouchMemory + 1;
            const int Reset = CheckMemory + 1;
            const int Renew = Reset + 1;

            int potentialActions = supportsRenew ? Renew : Reset;

            using (allocator)
            {
                allocator.Initialize(default(TOptions));

                var allocated = new List<HashedPointer>();
                for (int i = 0; i < 10000; i++)
                {
                    int action = rnd.Next(potentialActions + 1);

                    if (action < Deallocate) // We are allocating
                    {
                        int allocationSize = fixedSize != InvalidSize ? fixedSize : rnd.Next(16000) + 1;

                        Pointer ptr = allocator.Allocate(allocationSize);
                        var hash = Hashing.XXHash64.Calculate((byte*)ptr.Address, (ulong)ptr.Size);
                        allocated.Add(new HashedPointer { Hash = hash, Pointer = ptr });
                        continue;
                    }

                    if (allocated.Count == 0)
                        continue; // Nothing to do here. 

                    if (action == Deallocate || action == DeallocateLast)
                    {
                        int pointerIndex = action == DeallocateLast ? allocated.Count - 1 : rnd.Next(allocated.Count);

                        Pointer a = allocated[pointerIndex].Pointer;
                        allocated[pointerIndex] = allocated[allocated.Count - 1];
                        allocated.RemoveAt(allocated.Count - 1);

                        allocator.Release(ref a);
                    }
                    else if (action == TouchMemory)
                    {
                        for (int idx = 0; idx < allocated.Count; idx++)
                        {
                            var hashedPtr = allocated[idx];
                            var localPtr = hashedPtr.Pointer;
                            Assert.True(localPtr.IsValid);

                            int location = rnd.Next(localPtr.Size);
                            if (location >= localPtr.Size)
                                throw new InvalidOperationException();

                            var span = localPtr.AsSpan();
                            span[location] = 1;

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Address, (ulong)localPtr.Size);
                            hashedPtr.Hash = hash;
                            allocated[idx] = hashedPtr;
                        }
                    }
                    else if (action == CheckMemory)
                    {
                        for (int idx = 0; idx < allocated.Count; idx++)
                        {
                            var hashedPtr = allocated[idx];
                            var localPtr = hashedPtr.Pointer;
                            Assert.True(localPtr.IsValid);

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Address, (ulong)localPtr.Size);
                            Assert.Equal(hashedPtr.Hash, hash);
                        }
                    }
                    else if (action == Reset)
                    {
                        if (rnd.Next(10) == 0)
                        {
                            foreach (var block in allocated)
                            {
                                var ptr = block.Pointer;
                                allocator.Release(ref ptr);
                            }

                            allocated.Clear();
                            allocator.Reset();

                            Assert.Equal(0, allocator.TotalAllocated);
                            Assert.Equal(0, allocator.Allocated);
                            Assert.Equal(0, allocator.InUse);
                        }
                    }
                    else if (action == Renew)
                    {
                        if (rnd.Next(10) == 0)
                        {
                            foreach (var block in allocated)
                            {
                                var ptr = block.Pointer;
                                allocator.Release(ref ptr);
                            }

                            allocated.Clear();
                            allocator.Renew();

                            Assert.Equal(0, allocator.InUse);
                        }
                    }
                }

                foreach (var block in allocated)
                {
                    var ptr = block.Pointer;
                    allocator.Release(ref ptr);
                }

                Assert.Equal(0, allocator.InUse);
            }
        }

        private struct HashedBlockPointer
        {
            public ulong Hash;
            public BlockPointer Pointer;
        }

        private void StressBlockTester<TAllocator, TOptions>(BlockAllocator<TAllocator> allocator, int seed, bool supportsRenew = true)
            where TAllocator : struct, IAllocator<TAllocator, BlockPointer>
            where TOptions : struct, IAllocatorOptions
        {
            var rnd = new Random(seed);

            const int Allocate = 0;
            const int Deallocate = Allocate + 3;
            const int DeallocateLast = Deallocate + 1;
            const int TouchMemory = DeallocateLast + 1;
            const int CheckMemory = TouchMemory + 1;
            const int Reset = CheckMemory + 1;
            const int Renew = Reset + 1;

            int potentialActions = supportsRenew ? Renew : Reset;

            using (allocator)
            {
                allocator.Initialize(default(TOptions));

                var allocated = new List<HashedBlockPointer>();
                for (int i = 0; i < 1000; i++)
                {
                    int action = rnd.Next(potentialActions + 1);

                    if (action < Deallocate) // We are allocating
                    {
                        BlockPointer ptr = allocator.Allocate(rnd.Next(16000) + 1);
                        var hash = Hashing.XXHash64.Calculate((byte*)ptr.Address, (ulong)ptr.Size);
                        allocated.Add(new HashedBlockPointer { Hash = hash, Pointer = ptr });
                        continue;
                    }

                    if (allocated.Count == 0)
                        continue; // Nothing to do here. 

                    if (action == Deallocate || action == DeallocateLast)
                    {
                        int pointerIndex = action == DeallocateLast ? allocated.Count - 1 : rnd.Next(allocated.Count);

                        BlockPointer a = allocated[pointerIndex].Pointer;
                        allocated[pointerIndex] = allocated[allocated.Count - 1];
                        allocated.RemoveAt(allocated.Count - 1);

                        allocator.Release(ref a);
                    }
                    else if (action == TouchMemory)
                    {
                        for (int idx = 0; idx < allocated.Count; idx++)
                        {
                            var hashedPtr = allocated[idx];
                            var localPtr = hashedPtr.Pointer;
                            Assert.True(localPtr.IsValid);

                            int location = rnd.Next(localPtr.Size);
                            if (location >= localPtr.Size)
                                throw new InvalidOperationException();

                            var span = localPtr.AsSpan();
                            span[location] = 1;

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Address, (ulong)localPtr.Size);
                            hashedPtr.Hash = hash;
                            allocated[idx] = hashedPtr;
                        }
                    }
                    else if (action == CheckMemory)
                    {
                        for (int idx = 0; idx < allocated.Count; idx++)
                        {
                            var hashedPtr = allocated[idx];
                            var localPtr = hashedPtr.Pointer;
                            Assert.True(localPtr.IsValid);

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Address, (ulong)localPtr.Size);
                            Assert.Equal(hashedPtr.Hash, hash);
                        }
                    }
                    else if (action == Reset)
                    {
                        if (rnd.Next(10) == 0)
                        {
                            foreach (var block in allocated)
                            {
                                BlockPointer ptr = block.Pointer;
                                allocator.Release(ref ptr);
                            }

                            allocated.Clear();
                            allocator.Reset();

                            Assert.Equal(0, allocator.TotalAllocated);
                            Assert.Equal(0, allocator.Allocated);
                            Assert.Equal(0, allocator.InUse);
                        }
                    }
                    else if (action == Renew)
                    {
                        if (rnd.Next(10) == 0)
                        {
                            foreach (var block in allocated)
                            {
                                BlockPointer ptr = block.Pointer;
                                allocator.Release(ref ptr);
                            }

                            allocated.Clear();
                            allocator.Renew();

                            Assert.Equal(0, allocator.InUse);
                        }
                    }
                }

                foreach (var block in allocated)
                {
                    var ptr = block.Pointer;
                    allocator.Release(ref ptr);
                }

                Assert.Equal(0, allocator.InUse);
            }
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void Arena_Stress_Testing(int seed)
        {
            StressTester<ArenaAllocator<ArenaAllocator.Default>, ArenaAllocator.Default>(new Allocator<ArenaAllocator<ArenaAllocator.Default>>(), seed);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void Fragment_Stress_Testing(int seed)
        {
            StressTester<FragmentAllocator<FragmentAllocator.Default>, FragmentAllocator.Default>(new Allocator<FragmentAllocator<FragmentAllocator.Default>>(), seed, supportsRenew: false);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void Pool_Stress_Testing(int seed)
        {
            StressBlockTester<PoolAllocator<PoolAllocator.Default>, PoolAllocator.Default>(new BlockAllocator<PoolAllocator<PoolAllocator.Default>>(), seed, supportsRenew: false);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void NonBlockingPool_Stress_Testing(int seed)
        {
            StressBlockTester<NonBlockingPoolAllocator<PoolAllocator.Default>, PoolAllocator.Default>(new BlockAllocator<NonBlockingPoolAllocator<PoolAllocator.Default>>(), seed, supportsRenew: false);
        }

        public struct FixedSizeDefault : IPoolAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxBlockSize => 4 * Constants.Size.Kilobyte;
            public int MaxPoolSizeInBytes => 1 * Constants.Size.Megabyte;

            public bool HasOwnership => true;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<FixedSizeDefault>>();
                allocator.Initialize(default(FixedSizeDefault));
                return allocator;
            }

            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void FixedSizePool_Stress_Testing(int seed)
        {
            int blockSize = default(FixedSizeDefault).MaxBlockSize;
            StressTester<FixedSizePoolAllocator<FixedSizeDefault>, FixedSizeDefault>(new Allocator<FixedSizePoolAllocator<FixedSizeDefault>>(), seed, supportsRenew: false, fixedSize: blockSize);
        }

        public struct ThreadAffineDefault : IFixedSizeThreadAffinePoolOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int BlockSize => 4 * Constants.Size.Kilobyte;
            public int ItemsPerLane => 4;

            public bool AcceptOnlyBlocks => true;

            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;

            public bool HasOwnership => true;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<PoolAllocator.Default>>();
                allocator.Initialize(default(PoolAllocator.Default));
                return allocator;
            }

            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void FixedSizeThreadAffinePool_Stress_Testing(int seed)
        {
            int blockSize = default(ThreadAffineDefault).BlockSize;
            StressTester<FixedSizeThreadAffinePoolAllocator<ThreadAffineDefault>, ThreadAffineDefault>(new Allocator<FixedSizeThreadAffinePoolAllocator<ThreadAffineDefault>>(), seed, supportsRenew: true, fixedSize: blockSize);
        }
    }
}
