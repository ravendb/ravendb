using FastTests;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8427 : NoDisposalNeeded
    {
        public RavenDB_8427(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldCleanEscapePositionWhenTakenFromCache()
        {
            using (var pool = new JsonContextPool())
            {
                using (pool.AllocateOperationContext(out var context))
                {
                    var lazyString = context.GetLazyString("abc \b cba");
                    Assert.NotNull(lazyString.EscapePositions);
                }

                using (pool.AllocateOperationContext(out var context))
                {
                    var lazyString = context.GetLazyString("abc");
                    Assert.Null(lazyString.EscapePositions);
                }
            }
        }
    }
}
