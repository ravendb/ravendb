using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Global;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class AllocatorsParallelTests : NoDisposalNeeded
    {
        public struct Safe : IPoolAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxBlockSize => 64 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 256 * Constants.Size.Megabyte;

            public bool HasOwnership => true;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Safe>>();
                allocator.Initialize(default(Safe));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator?.Dispose(disposing);
            }
        }

        public struct Unsafe : IPoolAllocatorOptions, IArenaAllocatorOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxBlockSize => 64 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 256 * Constants.Size.Megabyte;

            public bool HasOwnership => true;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<ArenaAllocator<Unsafe>>();
                allocator.Initialize(default(Unsafe));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator?.Dispose(disposing);
            }

            public int InitialArenaSize => 6 * Constants.Size.Kilobyte;
            public int MaxArenaSize => 6 * Constants.Size.Kilobyte;
            public IArenaGrowthStrategy GrowthStrategy => default(ArenaAllocator.DoubleGrowthStrategy);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void SimultaneousThreadSafeUnderlyingAllocator(int seed)
        {
            var rnd = new Random(seed);

            int count = rnd.Next(100000) + 10;

            var allocated = new BlockPointer[count];
            using (var allocator = new BlockAllocator<NonBlockingPoolAllocator<Safe>>())
            {
                allocator.Initialize(default(Safe));

                var result = Parallel.For(0, count, i =>
                {
                    allocated[i] = allocator.Allocate(100);
                });

                Assert.True(result.IsCompleted);
                Assert.Equal(100 * count, allocator.InUse);
                Assert.True(allocator.InUse <= allocator.Allocated);
                Assert.Equal(100 * count, allocator.TotalAllocated);

                var found = new HashSet<long>();
                foreach (var ptr in allocated)
                {
                    // Force a write, if memory is allocated properly CLR wont die.
                    *(byte*)ptr.Address = 1;

                    // Check no race condition.
                    Assert.False(found.Contains((long)ptr.Address));
                    found.Add((long)ptr.Address);
                }

                result = Parallel.For(0, count, i =>
                {
                    allocator.Release(ref allocated[i]);
                });

                Assert.True(result.IsCompleted);
                Assert.Equal(0, allocator.InUse);
            }
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void SimultaneousNonThreadSafeUnderlyingAllocator(int seed)
        {
            var rnd = new Random(seed);

            int count = rnd.Next(100000) + 10;

            var allocated = new BlockPointer[count];
            using (var allocator = new BlockAllocator<NonBlockingPoolAllocator<Unsafe>>())
            {
                allocator.Initialize(default(Unsafe));

                var result = Parallel.For(0, count, i =>
                {
                    allocated[i] = allocator.Allocate(100);
                });

                Assert.True(result.IsCompleted);
                Assert.Equal(100 * count, allocator.InUse);
                Assert.True(allocator.InUse <= allocator.Allocated);
                Assert.Equal(100 * count, allocator.TotalAllocated);

                var found = new HashSet<long>();
                foreach (var ptr in allocated)
                {
                    // Force a write, if memory is allocated properly CLR wont die.
                    *(byte*)ptr.Address = 1;

                    // Check no race condition.
                    Assert.False(found.Contains((long)ptr.Address));
                    found.Add((long)ptr.Address);
                }

                result = Parallel.For(0, count, i =>
                {
                    allocator.Release(ref allocated[i]);
                });

                Assert.True(result.IsCompleted);
                Assert.Equal(0, allocator.InUse);
            }
        }

        public struct StubAllocator<TOptions> : IAllocator<StubAllocator<TOptions>, Pointer>, IAllocator, ILowMemoryHandler<StubAllocator<TOptions>>, IRenewable<StubAllocator<TOptions>>, ILifecycleHandler<StubAllocator<TOptions>>
            where TOptions : struct, INativeOptions
        {
            private TOptions Options;
            private long _generation;

            public void Configure<TConfig>(ref NativeAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
            {                
            }

            public long TotalAllocated { get; }

            public long Allocated
            {
                get;
                private set;
            }

            public long InUse { get; }

            public void Initialize(ref StubAllocator<TOptions> allocator)
            {
                _generation = 1;
            }

            public void Configure<TConfig>(ref StubAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
            {
            }

            public Pointer Allocate(ref StubAllocator<TOptions> allocator, int size)
            {
                return new Pointer((void*)_generation, size);
            }

            public void Release(ref StubAllocator<TOptions> allocator, ref Pointer ptr)
            {
            }

            public void Renew(ref StubAllocator<TOptions> allocator)
            {
                Thread.Sleep(5000);
                allocator._generation++;
            }

            public void Reset(ref StubAllocator<TOptions> allocator)
            {
                Thread.Sleep(5000);
                allocator._generation++;
            }

            public void OnAllocate(ref StubAllocator<TOptions> allocator, Pointer ptr)
            {
            }

            public void OnRelease(ref StubAllocator<TOptions> allocator, Pointer ptr)
            {
            }

            public void Dispose(ref StubAllocator<TOptions> allocator, bool disposing)
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

            public bool IsThreadSafe => true;

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

        public struct Blocking : IPoolAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxBlockSize => 64 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 256 * Constants.Size.Megabyte;

            public bool HasOwnership => true;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<StubAllocator<Blocking>>();
                allocator.Initialize(default(Blocking));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator?.Dispose(disposing);
            }
        }

        [Fact]
        public void SafeResetWait()
        {
            int count = 1000;

            var allocated = new BlockPointer[count];
            using (var allocator = new BlockAllocator<NonBlockingPoolAllocator<Blocking>>())
            {
                allocator.Initialize(default(Blocking));

                var task = Task.Run(() => allocator.Renew());
                var result = Parallel.For(0, count, i =>
                {
                    allocated[i] = allocator.Allocate(100);
                });

                Assert.True(task.Wait(10000));
                Assert.True(result.IsCompleted);

                foreach (var ptr in allocated)
                    Assert.Equal(1, (long)ptr.Address);

                task = Task.Run(() => allocator.Reset());
                result = Parallel.For(0, count, i =>
                {
                    allocated[i] = allocator.Allocate(100);
                });

                Assert.True(task.Wait(10000));
                Assert.True(result.IsCompleted);

                foreach (var ptr in allocated)
                    Assert.Equal(2, (long)ptr.Address);

                allocator.Reset();
            }
        }
    }
}
