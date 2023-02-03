using FastTests;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19913 : RavenLowLevelTestBase
{
    public RavenDB_19913(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void ContextAllocatedMemoryShouldTakeIntoAccountAllocationsMadeByByteStringContextAllocator()
    {
        using (var database = CreateDocumentDatabase())
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var oneMb = new Size(1, SizeUnit.Megabytes);

                context.GetMemory(64 * Constants.Size.Kilobyte);

                context.Allocator.Allocate(512 * Constants.Size.Kilobyte, out var buffer); // this will allocate 1MB under the covers

                Assert.Equal(oneMb, new Size(buffer.Size, SizeUnit.Bytes)); // precaution

                var allocated = new Size(context.AllocatedMemory, SizeUnit.Bytes);

                Assert.True(allocated >= oneMb, $"allocated isn't greater than 1MB - {allocated}");
            }
        }
    }
}
