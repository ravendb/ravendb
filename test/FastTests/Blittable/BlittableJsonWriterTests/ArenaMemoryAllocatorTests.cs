using System;
using Sparrow.Json;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable.BlittableJsonWriterTests;

public class ArenaMemoryAllocatorTests : NoDisposalNeeded
{
    public ArenaMemoryAllocatorTests(ITestOutputHelper output) : base(output)
    {

    }

    [Fact]
    public void ShouldUseFragmentedMemorySegment()
    {
        using (var arena = new ArenaMemoryAllocator(SharedMultipleUseFlag.None))
        {
            const int baseAllocationSize = 4096;

            var allocation = arena.Allocate(4096);
            var result = arena.GrowAllocation(allocation, baseAllocationSize);
            Assert.True(result);

            // creating fragmentation
            _ = arena.Allocate(32);

            arena.Return(allocation);

            var newAllocation = arena.Allocate(baseAllocationSize);

            var totalUsedBefore = arena.TotalUsed;

            result = arena.GrowAllocation(newAllocation, baseAllocationSize);
            Assert.False(result);

            // should use from cache
            arena.Allocate(baseAllocationSize * 2);

            Assert.Equal(arena.TotalUsed, totalUsedBefore);
        }


        using (var context = new JsonOperationContext(1024 * 64, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
        {
            BlittableJsonReaderObject blittable;
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                builder.StartWriteObjectDocument();
                builder.StartWriteObject();

                for (int i = 0; i < 140; i++)
                {
                    builder.WritePropertyName("Data" + i);
                    builder.StartWriteObject();
                    builder.WritePropertyName("Age" + i);
                    builder.WriteValue(36);
                }

                for (int i = 0; i < 140; i++)
                {
                    builder.WriteObjectEnd();
                }

                builder.WriteObjectEnd();
                builder.FinalizeDocument();
                blittable = builder.CreateReader();
            }

            const int fragmentationSize = 16;
            long memoryUsedBefore = 0;

            for (var i = 0; i < 3; i++)
            {
                memoryUsedBefore = context.AllocatedMemory;

                BuildDocument(() =>
                {
                    // create fragmentation
                    context.GetMemory(fragmentationSize);
                });
            }

            Assert.Equal(memoryUsedBefore + fragmentationSize, context.AllocatedMemory);

            void BuildDocument(Action beforeBuilderDispose = null)
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                    builder.StartWriteObjectDocument();
                    builder.StartWriteObject();

                    builder.WritePropertyName("Embedded");
                    builder.WriteEmbeddedBlittableDocument(blittable);

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    beforeBuilderDispose?.Invoke();
                }
            }
        }
    }
}
