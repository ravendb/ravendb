﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class AllocatorsTests : NoDisposalNeeded
    {
        [Fact]
        public void Alloc_NativeDefaultByBytes()
        {
            var allocator = new Allocator<NativeBlockAllocator<NativeBlockAllocator.Default>>();
            allocator.Initialize(default(NativeBlockAllocator.Default));

            var ptr = allocator.Allocate(1000);
            Assert.Equal(1000, ptr.Size);
            Assert.True(ptr.IsValid);

            allocator.Release(ptr);
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
            var allocator = new Allocator<NativeBlockAllocator<NativeBlockAllocator.Default>>();
            allocator.Initialize(default(NativeBlockAllocator.Default));

            var ptr = allocator.Allocate<MyStruct>(10);
            Assert.Equal(10, ptr.Size);
            Assert.Equal(10 * sizeof(MyStruct), ptr.SizeAsBytes);
            Assert.True(ptr.IsValid);

            allocator.Release(ptr);
            Assert.False(ptr.IsValid);
        }

        [Fact]
        public void Alloc_NativeSpanByByte_Whole()
        {
            var allocator = new Allocator<NativeBlockAllocator<NativeBlockAllocator.Default>>();
            allocator.Initialize(default(NativeBlockAllocator.Default));

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
            var allocator = new Allocator<NativeBlockAllocator<NativeBlockAllocator.DefaultZero>>();
            allocator.Initialize(default(NativeBlockAllocator.DefaultZero));

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

        [Fact]
        public void Alloc_NativeUnsupported()
        {
            var allocator = new Allocator<NativeBlockAllocator<NativeBlockAllocator.Default>>();
            Assert.Throws<NotSupportedException>(() => allocator.Initialize(default(NativeBlockAllocator.DefaultZero)));

            allocator.Initialize(default(NativeBlockAllocator.Default));

            Assert.Throws<NotSupportedException>(() => allocator.Reset());
            Assert.Throws<NotSupportedException>(() => allocator.Renew());
        }

        [Fact]
        public void Alloc_StubLifecycle()
        {
            var allocator = new Allocator<StubAllocator<NativeBlockAllocator.Default>>();         
            allocator.Initialize(default(NativeBlockAllocator.Default));

            Assert.Throws<NotSupportedException>(() => allocator.Reset());
            Assert.Throws<NotSupportedException>(() => allocator.Renew());

            Assert.Throws<NotSupportedException>(() => allocator.LowMemory());
            Assert.Throws<NotSupportedException>(() => allocator.LowMemoryOver());
        }

        public struct StubAllocator<TOptions> : IAllocator<StubAllocator<TOptions>>, IAllocator, IDisposable, ILowMemoryHandler<StubAllocator<TOptions>>, IRenewable<StubAllocator<TOptions>>, ILifecycleHandler<StubAllocator<TOptions>>
            where TOptions : struct, INativeBlockOptions
        {
            private TOptions Options;

            public void Configure<TConfig>(ref NativeBlockAllocator<TOptions> blockAllocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
            {
            }

            public int Allocated
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

            public void Allocate(ref StubAllocator<TOptions> allocator, int size, out BlockPointer.Header* header)
            {
                header = null;
            }

            public void Release(ref StubAllocator<TOptions> allocator, in BlockPointer.Header* header)
            {                
            }

            public void Renew(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
            }

            public void Reset(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
            }
  
            public void Dispose()
            {
            }

            public void NotifyLowMemory(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
            }

            public void NotifyLowMemoryOver(ref StubAllocator<TOptions> allocator)
            {
                throw new NotSupportedException($"{nameof(NativeBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
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

    }
}
