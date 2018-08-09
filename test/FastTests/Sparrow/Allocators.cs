using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Esprima;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Global;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class AllocatorsTests : NoDisposalNeeded
    {
        [Fact]
        public void Alloc_NativeDefaultByBytes()
        {
            var allocator = new Allocator<NativeAllocator<NativeAllocator.Default>>();
            allocator.Initialize(default(NativeAllocator.Default));

            var ptr = allocator.Allocate(1000);
            Assert.Equal(1000, ptr.Size);
            Assert.True(ptr.IsValid);

            allocator.Release(ref ptr);
            Assert.False(ptr.IsValid);
        }

        [StructLayout(LayoutKind.Explicit, Size = 16)]
        private struct MyStruct
        {
            [FieldOffset(4)]
            public int Value;
        }

        [Fact]
        public void Alloc_NativeDefaultByType()
        {
            var allocator = new Allocator<NativeAllocator<NativeAllocator.Default>>();
            allocator.Initialize(default(NativeAllocator.Default));

            var ptr = allocator.Allocate<MyStruct>(10);
            Assert.Equal(10, ptr.Size);
            Assert.Equal(10 * sizeof(MyStruct), ptr.SizeAsBytes);
            Assert.True(ptr.IsValid);

            allocator.Release(ref ptr);
            Assert.False(ptr.IsValid);
        }

        [Fact]
        public void Alloc_NativeSpanByByte_Whole()
        {
            var allocator = new Allocator<NativeAllocator<NativeAllocator.Default>>();
            allocator.Initialize(default(NativeAllocator.Default));

            var ptr = allocator.Allocate(1000);

            var span = ptr.AsSpan();
            Assert.Equal(span.Length, 1000);
            Assert.False(span.IsEmpty);
            for (int i = 0; i < span.Length; i++)
                span[i] = 3;

            byte* nakedPtr = (byte*)ptr.Ptr;
            for (int i = 0; i < 1000; i++)
                Assert.Equal(3, nakedPtr[i]);
        }

        [Fact]
        public void Alloc_NativeSpanByByte_Chunk()
        {
            var allocator = new Allocator<NativeAllocator<NativeAllocator.DefaultZero>>();
            allocator.Initialize(default(NativeAllocator.DefaultZero));

            var ptr = allocator.Allocate(1000);

            var span = ptr.AsSpan(100, 100);
            Assert.Equal(span.Length, 100);
            Assert.False(span.IsEmpty);
            for (int i = 0; i < span.Length; i++)
                span[i] = 3;

            var whole = ptr.AsSpan();
            for (int i = 0; i < span.Length; i++)
                Assert.Equal(i >= 100 && i < 200 ? 3 : 0, whole[i]);

            span = ptr.AsSpan(200);
            for (int i = 0; i < span.Length; i++)
                span[i] = 3;

            for (int i = 0; i < span.Length; i++)
                Assert.Equal(i < 200 ? 3 : 0, whole[i]);
        }

        //[Fact]
        //public void Alloc_NativeSpanByByte_4KAligned()
        //{
        //    var allocator = new Allocator<NativeAllocator<NativeAllocator.Default4K>>();
        //    allocator.Initialize(default(NativeAllocator.Default4K));

        //    var ptr = allocator.Allocate(1000);
        //    Assert.True((long)ptr.Ptr % 4096 == 0);
        //    Assert.Equal(1000, ptr.Size);
        //    Assert.True(ptr.IsValid);

        //    allocator.Release(ptr);
        //    Assert.False(ptr.IsValid);
        //}

        [Fact]
        public void Alloc_NativeUnsupported()
        {
            var allocator = new Allocator<NativeAllocator<NativeAllocator.Default>>();
            Assert.Throws<NotSupportedException>(() => allocator.Initialize(default(NativeAllocator.DefaultZero)));

            allocator.Initialize(default(NativeAllocator.Default));

            Assert.Throws<NotSupportedException>(() => allocator.Reset());
            Assert.Throws<NotSupportedException>(() => allocator.Renew());
        }

        [Fact]
        public void Alloc_PoolDefaultByBytes()
        {
            var allocator = new BlockAllocator<PoolAllocator<PoolAllocator.Default>>();
            allocator.Initialize(default(PoolAllocator.Default));

            var ptr = allocator.Allocate(1000);
            Assert.Equal(1000, ptr.Size);
            Assert.True(ptr.IsValid);

            allocator.Release(ref ptr);
            Assert.False(ptr.IsValid);
        }

        [Fact]
        public void Alloc_PoolReturnUsedBytes()
        {
            var allocator = new BlockAllocator<PoolAllocator<PoolAllocator.Default>>();
            allocator.Initialize(default(PoolAllocator.Default));

            int size = 1000;

            var ptr = allocator.Allocate(1024);
            Assert.Equal(1024, ptr.BlockSize);
            Assert.True(ptr.IsValid);

            long pointerAddress = (long)ptr.Ptr;

            allocator.Release(ref ptr);
            Assert.False(ptr.IsValid);

            ptr = allocator.Allocate(size);
            Assert.Equal(size, ptr.Size);
            Assert.True(ptr.IsValid);

            Assert.Equal(pointerAddress, (long)ptr.Ptr);
        }

        [Fact]
        public void Alloc_PoolReturnBlockBytes()
        {
            var allocator = new BlockAllocator<PoolAllocator<PoolAllocator.Default>>();
            allocator.Initialize(default(PoolAllocator.Default));

            int size = 1000;

            long[] addresses = new long[5];
            var pointers = new BlockPointer[5];
            for (int i = 0; i < 5; i++)
            {
                var ptr = allocator.Allocate(1024);
                Assert.Equal(1024, ptr.Size);
                Assert.True(ptr.IsValid);
                Assert.Equal(1024, ptr.BlockSize);

                pointers[i] = ptr;
                addresses[i] = (long)ptr.Ptr;
            }

            for (int i = 0; i < 5; i++)
            {
                allocator.Release(ref pointers[i]);
                Assert.False(pointers[i].IsValid);
            }

            for (int i = 0; i < 5; i++)
            {
                var ptr = allocator.Allocate(size);
                Assert.Contains((long)ptr.Ptr, addresses);
                Assert.Equal(1024, ptr.BlockSize);
            }

            var nonReusedPtr = allocator.Allocate(size);
            Assert.Equal(size, nonReusedPtr.Size);
            Assert.Equal(size, nonReusedPtr.BlockSize);
            Assert.True(nonReusedPtr.IsValid);
            // Cannot check for actual different addresses because the memory system may return it back to us again. 
        }


        [Fact]
        public void Alloc_ThreadAffinePoolReturnUsedBytes()
        {
            var allocator = new FixedSizeAllocator<FixedSizeThreadAffinePoolAllocator<FixedSizeThreadAffinePoolAllocator.Default>>();
            allocator.Initialize(default(FixedSizeThreadAffinePoolAllocator.Default));

            var config = default(FixedSizeThreadAffinePoolAllocator.Default);

            var ptr = allocator.Allocate();
            Assert.Equal(config.BlockSize, ptr.Size);
            Assert.True(ptr.IsValid);

            long pointerAddress = (long)ptr.Ptr;

            allocator.Release(ref ptr);
            Assert.False(ptr.IsValid);

            ptr = allocator.Allocate();
            Assert.Equal(config.BlockSize, ptr.Size);
            Assert.True(ptr.IsValid);

            Assert.Equal(pointerAddress, (long)ptr.Ptr);
        }


        public struct FixedSize : IArenaAllocatorOptions
        {
            private struct Internal : IArenaGrowthStrategy
            {
                public int GetPreferredSize(long allocated, long used)
                {
                    return 10 * Constants.Size.Kilobyte;
                }

                public int GetGrowthSize(long allocated, long used)
                {
                    return 10 * Constants.Size.Kilobyte;
                }
            }

            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            // TODO: Check if this call gets devirtualized. 
            public IArenaGrowthStrategy GrowthStrategy => default(Internal);
            public int InitialArenaSize => 1 * Constants.Size.Megabyte;
            public int MaxArenaSize => 64 * Constants.Size.Megabyte;
        }

        public struct FragmentFixedSize : IFragmentAllocatorOptions
        {
            public int ReuseBlocksBiggerThan => 1 * Constants.Size.Kilobyte;
            public int BlockSize => 10 * Constants.Size.Kilobyte;
            public IAllocatorComposer<Pointer> CreateAllocator() => new Allocator<NativeAllocator<FixedSize>>();
        }

        [Fact]
        public void Alloc_ArenaReturnBlockBytes()
        {
            var allocator = new Allocator<ArenaAllocator<FixedSize>>();
            allocator.Initialize(default(FixedSize));

            int size = 1000;

            long[] addresses = new long[5];
            var pointers = new Pointer[5];
            for (int i = 0; i < 5; i++)
            {
                var ptr = allocator.Allocate(size);
                Assert.Equal(size, ptr.Size);
                Assert.True(ptr.IsValid);

                pointers[i] = ptr;
                addresses[i] = (long)ptr.Ptr;
            }

            for (int i = 4; i >= 0; i--)
            {
                allocator.Release(ref pointers[i]);
                Assert.False(pointers[i].IsValid);
            }

            for (int i = 0; i < 4; i++)
            {
                var ptr = allocator.Allocate(size);
                Assert.Contains((long)ptr.Ptr, addresses);
            }

            var nonReusedPtr = allocator.Allocate(size);
            Assert.Equal(size, nonReusedPtr.Size);
            Assert.True(nonReusedPtr.IsValid);
            // Cannot check for actual different addresses because the memory system may return it back to us again. 
        }

        [Fact]
        public void Alloc_ArenaGrowMultiple()
        {
            var allocator = new Allocator<ArenaAllocator<FixedSize>>();
            allocator.Initialize(default(FixedSize));

            int size = 9000;
            Pointer ptr;

            int length = 10;

            long[] addresses = new long[length];
            var pointers = new Pointer[length];
            for (int i = 0; i < length; i++)
            {
                ptr = allocator.Allocate(size);
                Assert.Equal(size, ptr.Size);
                Assert.True(ptr.IsValid);

                pointers[i] = ptr;
                addresses[i] = (long)ptr.Ptr;
            }

            for (int i = length - 1; i >= 0; i--)
            {
                allocator.Release(ref pointers[i]);
                Assert.False(pointers[i].IsValid);
            }

            allocator.Reset();

            ptr = allocator.Allocate(size);
            Assert.Equal(size, ptr.Size);
            Assert.True(ptr.IsValid);
        }

        [Fact]
        public void Alloc_FragmentGrowMultiple()
        {
            var allocator = new Allocator<FragmentAllocator<FragmentFixedSize>>();
            allocator.Initialize(default(FragmentFixedSize));

            int size = 4000;
            Pointer ptr;

            int length = 10;

            long[] addresses = new long[length];
            var pointers = new Pointer[length];
            for (int i = 0; i < length; i++)
            {
                ptr = allocator.Allocate(size);
                Assert.Equal(size, ptr.Size);
                Assert.True(ptr.IsValid);

                pointers[i] = ptr;
                addresses[i] = (long)ptr.Ptr;
            }

            for (int i = length - 1; i >= 0; i--)
            {
                allocator.Release(ref pointers[i]);
                Assert.False(pointers[i].IsValid);
            }

            allocator.Reset();

            ptr = allocator.Allocate(size);
            Assert.Equal(size, ptr.Size);
            Assert.True(ptr.IsValid);
        }

        [Fact]
        public void Alloc_ThreadAffinePoolReturnBlockBytes()
        {
            var allocator = new FixedSizeAllocator<FixedSizeThreadAffinePoolAllocator<FixedSizeThreadAffinePoolAllocator.Default>>();
            allocator.Initialize(default(FixedSizeThreadAffinePoolAllocator.Default));

            var config = default(FixedSizeThreadAffinePoolAllocator.Default);

            long[] addresses = new long[5];
            var pointers = new Pointer[5];
            for (int i = 0; i < 5; i++)
            {
                var ptr = allocator.Allocate();
                Assert.Equal(config.BlockSize, ptr.Size);
                Assert.True(ptr.IsValid);

                pointers[i] = ptr;
                addresses[i] = (long)ptr.Ptr;
            }

            for (int i = 0; i < 5; i++)
            {
                allocator.Release(ref pointers[i]);
                Assert.False(pointers[i].IsValid);
            }

            for (int i = 0; i < 4; i++)
            {
                var ptr = allocator.Allocate();
                Assert.Contains((long)ptr.Ptr, addresses);
            }

            var nonReusedPtr = allocator.Allocate();
            Assert.Equal(config.BlockSize, nonReusedPtr.Size);
            Assert.True(nonReusedPtr.IsValid);
            // Cannot check for actual different addresses because the memory system may return it back to us again. 
        }

        [Fact]
        public void Alloc_StubLifecycle()
        {
            var allocator = new Allocator<StubAllocator<NativeAllocator.Default>>();         
            allocator.Initialize(default(NativeAllocator.Default));

            Assert.Throws<NotSupportedException>(() => allocator.Reset());
            Assert.Throws<NotSupportedException>(() => allocator.Renew());

            Assert.Throws<NotSupportedException>(() => allocator.LowMemory());
            Assert.Throws<NotSupportedException>(() => allocator.LowMemoryOver());
        }

        public struct StubAllocator<TOptions> : IAllocator<StubAllocator<TOptions>, Pointer>, IAllocator, ILowMemoryHandler<StubAllocator<TOptions>>, IRenewable<StubAllocator<TOptions>>, ILifecycleHandler<StubAllocator<TOptions>>
            where TOptions : struct, INativeOptions
        {
            private TOptions Options;

            public void Configure<TConfig>(ref NativeAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
            {
            }

            public long Allocated
            {
                get;
                private set;
            }

            public void Initialize(ref StubAllocator<TOptions> allocator)
            {               
            }

            public void Configure<TConfig>(ref StubAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
            {             
            }

            public Pointer Allocate(ref StubAllocator<TOptions> allocator, int size)
            {
                return new Pointer();
            }

            public void Release(ref StubAllocator<TOptions> allocator, ref Pointer ptr)
            {                
            }

            public void Renew(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
            }

            public void Reset(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
            }

            public void OnAllocate(ref StubAllocator<TOptions> allocator, Pointer ptr)
            {
            }

            public void OnRelease(ref StubAllocator<TOptions> allocator, Pointer ptr)
            {
            }

            public void Dispose(ref StubAllocator<TOptions> allocator)
            {
            }

            public void NotifyLowMemory(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
            }

            public void NotifyLowMemoryOver(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
            }

            public bool BeforeInitializedCalled;

            public void BeforeInitialize(ref StubAllocator<TOptions> allocator)
            {
                BeforeInitializedCalled = true;
            }

            public bool AfterInitializeCalled;

            public void AfterInitialize(ref StubAllocator<TOptions> allocator)
            {
                AfterInitializeCalled = true;
            }

            public bool BeforeDisposeCalled;

            public void BeforeDispose(ref StubAllocator<TOptions> allocator)
            {
                BeforeDisposeCalled = true;
            }

            public bool BeforeFinalizationCalled;

            public void BeforeFinalization(ref StubAllocator<TOptions> allocator)
            {
                BeforeFinalizationCalled = true;
            }
        }

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
                for (int i = 0; i < 1000000; i++)
                {
                    int action = rnd.Next(potentialActions + 1);

                    if (action < Deallocate) // We are allocating
                    {
                        int allocationSize = fixedSize != InvalidSize ? fixedSize : rnd.Next(16000) + 1;

                        Pointer ptr = allocator.Allocate(allocationSize);
                        var hash = Hashing.XXHash64.Calculate((byte*)ptr.Ptr, (ulong)ptr.Size);
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

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Ptr, (ulong)localPtr.Size);
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

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Ptr, (ulong)localPtr.Size);
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
                        }
                    }
                }

                foreach (var block in allocated)
                {
                    var ptr = block.Pointer;
                    allocator.Release(ref ptr);
                }
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
                for (int i = 0; i < 1000000; i++)
                {
                    int action = rnd.Next(potentialActions + 1);

                    if (action < Deallocate) // We are allocating
                    {
                        BlockPointer ptr = allocator.Allocate(rnd.Next(16000) + 1);
                        var hash = Hashing.XXHash64.Calculate((byte*)ptr.Ptr, (ulong)ptr.Size);
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

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Ptr, (ulong)localPtr.Size);
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

                            var hash = Hashing.XXHash64.Calculate((byte*)localPtr.Ptr, (ulong)localPtr.Size);
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
                        }
                    }
                }

                foreach (var block in allocated)
                {
                    var ptr = block.Pointer;
                    allocator.Release(ref ptr);
                }
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
        public void FixedSize_Stress_Testing(int seed)
        {
            int blockSize = default(FixedSizePoolAllocator.Default).MaxBlockSize;
            StressTester<FixedSizePoolAllocator<FixedSizePoolAllocator.Default>, FixedSizePoolAllocator.Default>(new Allocator<FixedSizePoolAllocator<FixedSizePoolAllocator.Default>>(), seed, supportsRenew: false, fixedSize: blockSize);
        }

        

        [Theory]
        [InlineDataWithRandomSeed]
        public void Pool_Stress_Testing(int seed)
        {
            StressBlockTester<PoolAllocator<PoolAllocator.Default>, PoolAllocator.Default>(new BlockAllocator<PoolAllocator<PoolAllocator.Default>>(), seed, supportsRenew: false);
        }

    }
}
