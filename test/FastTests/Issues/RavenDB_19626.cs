using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_19626 : NoDisposalNeeded
{
    public RavenDB_19626(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void Should_Be_Able_To_Clone_Slice_Properly()
    {
        using (var allocator = new ByteStringContext(new SharedMultipleUseFlag()))
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (Slice.From(allocator, "clone_me", out var input))
        {
            // previously we were using here Slice.CloneToJsonContext
            var clone = input.Clone(allocator);

            Assert.Equal(0, SliceComparer.Compare(input, clone));
        }
    }
}
