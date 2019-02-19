using System;
using Sparrow.Json;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class UnmanagedWriteBufferTests : NoDisposalNeeded
    {
        private const int DefaultBufferSize = 16;

        private static readonly byte[] NoAllocationsBatch;
        private static readonly byte[] AllocationsBatch;

        static UnmanagedWriteBufferTests()
        {
            NoAllocationsBatch = new byte[DefaultBufferSize - 1];
            for (int i = 0; i < NoAllocationsBatch.Length; i++)
                NoAllocationsBatch[i] = (byte)i;

            AllocationsBatch = new byte[2 * DefaultBufferSize];
            for (int i = 0; i < AllocationsBatch.Length; i++)
                AllocationsBatch[i] = (byte)i;
        }

        [Fact]
        public void WriteSingleByteNoAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(2);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    Assert.Equal(buffer.SizeInBytes, 0);
                    buffer.WriteByte(10);
                    Assert.Equal(buffer.SizeInBytes, 1);
                    Assert.Equal(allocation.Address[0], 10);
                }
            }
        }

        [Fact]
        public void WriteTwoBytesWithAllocation()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(1);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    Assert.Equal(buffer.SizeInBytes, 0);
                    buffer.WriteByte(10);
                    Assert.Equal(buffer.SizeInBytes, 1);
                    Assert.Equal(allocation.Address[0], 10);
                    buffer.WriteByte(20);
                    Assert.Equal(buffer.SizeInBytes, 2);
                }
            }
        }

        [Fact]
        public void WriteMultipleBytesNoAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(DefaultBufferSize);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    for (int i = 0; i < DefaultBufferSize - 1; i++)
                    {
                        Assert.Equal(buffer.SizeInBytes, i);
                        buffer.WriteByte((byte)i);
                        Assert.Equal(allocation.Address[i], (byte)i);
                        Assert.Equal(buffer.SizeInBytes, i + 1);
                    }
                }
            }
        }

        [Fact]
        public void WriteMultipleBytesWithAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(DefaultBufferSize);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    for (int i = 0; i < 2*DefaultBufferSize; i++)
                    {
                        Assert.Equal(buffer.SizeInBytes, i);
                        buffer.WriteByte((byte)i);
                        Assert.Equal(buffer.SizeInBytes, i + 1);

                        // We can't verify that anything after the allocation was actually put into the correct place
                        if (i < DefaultBufferSize)
                            Assert.Equal(allocation.Address[i], (byte)i);
                    }
                }
            }
        }

        [Fact]
        public void WriteMultipleBytesInBatchNoAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(DefaultBufferSize);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    Assert.Equal(buffer.SizeInBytes, 0);
                    buffer.Write(NoAllocationsBatch, 0, NoAllocationsBatch.Length);
                    Assert.Equal(buffer.SizeInBytes, NoAllocationsBatch.Length);

                    for (int i = 0; i < NoAllocationsBatch.Length; i++)
                        Assert.Equal(allocation.Address[i], NoAllocationsBatch[i]);
                }
            }
        }

        [Fact]
        public void WriteMultipleBytesInBatchWithAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(DefaultBufferSize);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    Assert.Equal(buffer.SizeInBytes, 0);
                    buffer.Write(AllocationsBatch, 0, AllocationsBatch.Length);
                    Assert.Equal(buffer.SizeInBytes, AllocationsBatch.Length);

                    for (int i = 0; i < DefaultBufferSize; i++)
                        Assert.Equal(allocation.Address[i], AllocationsBatch[i]);
                }
            }
        }

        [Fact]
        public void CopyToNoAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(DefaultBufferSize);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    Assert.Equal(buffer.SizeInBytes, 0);
                    buffer.Write(NoAllocationsBatch, 0, NoAllocationsBatch.Length);
                    Assert.Equal(buffer.SizeInBytes, NoAllocationsBatch.Length);

                    for (int i = 0; i < NoAllocationsBatch.Length; i++)
                        Assert.Equal(allocation.Address[i], NoAllocationsBatch[i]);


                    byte[] outputBuffer = new byte[NoAllocationsBatch.Length];
                    fixed (byte* outputBufferPtr = outputBuffer)
                        buffer.CopyTo(outputBufferPtr);
                    for (int i = 0; i < NoAllocationsBatch.Length; i++)
                        Assert.Equal(outputBuffer[i], NoAllocationsBatch[i]);
                }
            }
        }

        [Fact]
        public void CopyToWithAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(DefaultBufferSize);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    Assert.Equal(buffer.SizeInBytes, 0);
                    buffer.Write(NoAllocationsBatch, 0, NoAllocationsBatch.Length);
                    Assert.Equal(buffer.SizeInBytes, NoAllocationsBatch.Length);

                    for (var i = 0; i < NoAllocationsBatch.Length; i++)
                        Assert.Equal(allocation.Address[i], NoAllocationsBatch[i]);


                    byte[] outputBuffer = new byte[NoAllocationsBatch.Length];
                    fixed (byte* outputBufferPtr = outputBuffer)
                        buffer.CopyTo(outputBufferPtr);
                    for (var i = 0; i < NoAllocationsBatch.Length; i++)
                        Assert.Equal(outputBuffer[i], NoAllocationsBatch[i]);
                }
            }
        }

        [Fact]
        public void ClearResetsSizeAndEffectivellyClears()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allocation = context.GetMemory(DefaultBufferSize);

                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    Assert.Equal(buffer.SizeInBytes, 0);
                    buffer.Write(AllocationsBatch, 0, AllocationsBatch.Length);
                    Assert.Equal(buffer.SizeInBytes, AllocationsBatch.Length);
                    buffer.Clear();
                    Assert.Equal(buffer.SizeInBytes, 0);

                    byte[] outputBuffer = new byte[AllocationsBatch.Length];
                    for (var i = 0; i < outputBuffer.Length; i++)
                        outputBuffer[i] = 124;
                    fixed (byte* outputBufferPtr = outputBuffer)
                        buffer.CopyTo(outputBufferPtr);
                    foreach (var b in outputBuffer)
                        Assert.Equal(b, 124);
                }
            }
        }

        [Fact]
        public void RepeatedDisposeDoesNotThrow()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize));
                Assert.False(buffer.IsDisposed);
                buffer.Dispose();
                Assert.True(buffer.IsDisposed);
                buffer.Dispose();
                Assert.True(buffer.IsDisposed);
            }
        }

        [Fact]
        public void RepeatedDisposeOnDistinctCopiesDoesNotThrow()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize));
                Assert.False(buffer.IsDisposed);
                var bufferCopy = buffer;
                Assert.False(buffer.IsDisposed);
                Assert.False(bufferCopy.IsDisposed);
                buffer.Dispose();
                Assert.True(buffer.IsDisposed);
                Assert.True(bufferCopy.IsDisposed);
                bufferCopy.Dispose();
                Assert.True(buffer.IsDisposed);
                Assert.True(bufferCopy.IsDisposed);
            }
        }

        [Fact]
        public void RepeatedDisposeOnDistinctCopiesDoesNotThrowMirror()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize));
                Assert.False(buffer.IsDisposed);
                var bufferCopy = buffer;
                Assert.False(buffer.IsDisposed);
                Assert.False(bufferCopy.IsDisposed);
                bufferCopy.Dispose();
                Assert.True(buffer.IsDisposed);
                Assert.True(bufferCopy.IsDisposed);
                buffer.Dispose();
                Assert.True(buffer.IsDisposed);
                Assert.True(bufferCopy.IsDisposed);
            }
        }

        [Fact]
        public void CopiedDisposedObjectRemainsDisposed()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize));
                Assert.False(buffer.IsDisposed);
                buffer.Dispose();
                Assert.True(buffer.IsDisposed);
                var bufferCopy = buffer;
                Assert.True(buffer.IsDisposed);
                Assert.True(bufferCopy.IsDisposed);
            }
        }

        [Fact]
        public void WriteByteThrowsAfterDispose()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize));
                buffer.Dispose();
#if DEBUG
                Assert.Throws(typeof(ObjectDisposedException), () => buffer.WriteByte(10));
#else
                Assert.Throws(typeof(NullReferenceException), () => buffer.WriteByte(10));
#endif

            }
        }

        [Fact]
        public void WriteThrowsAfterDispose()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize));
                buffer.Dispose();


#if DEBUG
                Assert.Throws(typeof(ObjectDisposedException), () => buffer.Write(AllocationsBatch, 0, AllocationsBatch.Length));
#else
                Assert.Throws(typeof(NullReferenceException), () => buffer.Write(AllocationsBatch, 0, AllocationsBatch.Length));
#endif
            }
        }

        [Fact]
        public void CopyToThrowsAfterDispose()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize));
                buffer.Dispose();

                var outputBuffer = new byte[DefaultBufferSize];
                fixed (byte* outputBufferPtr = outputBuffer)
                {
#if DEBUG
                    try
                    {
                        buffer.CopyTo(outputBufferPtr);
                        Assert.False(true);
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    catch (Exception)
                    {
                        Assert.False(true);
                    }
#else
                    try
                    {
                        buffer.CopyTo(outputBufferPtr);
                        Assert.False(true);
                    }
                    catch (NullReferenceException)
                    {
                    }
                    catch (Exception)
                    {
                        Assert.False(true);
                    }
#endif
                }
            }
        }

        [Fact]
        public void EnsureSingleChunkDoesNotChangeSizeOrContentsWithAllocations()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(DefaultBufferSize)))
                {
                    buffer.Write(AllocationsBatch, 0, AllocationsBatch.Length);

                    buffer.EnsureSingleChunk(out byte* address, out int size);
                    Assert.Equal(size, AllocationsBatch.Length);
                    for (int i = 0; i < AllocationsBatch.Length; i++)
                        Assert.Equal(address[i], AllocationsBatch[i]);

                    var outputBuffer = new byte[AllocationsBatch.Length];
                    fixed (byte* outputBufferPtr = outputBuffer)
                        buffer.CopyTo(outputBufferPtr);
                    for (int i = 0; i < AllocationsBatch.Length; i++)
                        Assert.Equal(outputBuffer[i], AllocationsBatch[i]);
                }
            }
        }

        [Fact]
        public void DoesNotLeakMemoryWhenClearing()
        {
            int Size = 1024;

            byte[] dataToWrite = new byte[Size];
            for (int i = 0; i < dataToWrite.Length; i++)
                dataToWrite[i] = (byte)i;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                // This is the amount allocated by the operation context by default
                var expectedAllocations = context.AllocatedMemory;

                var allocation = context.GetMemory(Size / 2); // Since we start with half the amount we need, it will require an allocation
                using (var buffer = new UnmanagedWriteBuffer(context, allocation))
                {
                    buffer.Write(dataToWrite, 0, Size);
                    buffer.Clear();
                }
                
                Assert.Equal(context.AllocatedMemory, expectedAllocations);

            }
        }

        [Fact]
        public void BehavesWithRespectToCopying()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (var buffer = new UnmanagedWriteBuffer(context, context.GetMemory(8)))
                {
                    buffer.WriteByte(1);
                    var bufferCopy = buffer;
                    Assert.Equal(bufferCopy.SizeInBytes, buffer.SizeInBytes);

                    bufferCopy.WriteByte(2);
                    Assert.Equal(bufferCopy.SizeInBytes, buffer.SizeInBytes);

                    buffer.WriteByte(3);
                    Assert.Equal(bufferCopy.SizeInBytes, buffer.SizeInBytes);
                }

            }
        }


    }
}
