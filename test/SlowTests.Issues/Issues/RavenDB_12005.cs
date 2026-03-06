using FastTests;
using Sparrow.Json;
using Xunit;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_12005 : RavenTestBase
    {
        public RavenDB_12005(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public unsafe void LastIndexOfInLazyStringValueShouldWork()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var lsv = context.GetLazyString("ord/1-A");
                var idx = lsv.LastIndexOf('/');
                Assert.Equal(3, idx);
                idx = lsv.LastIndexOfAny(new[] { '/' });
                Assert.Equal(3, idx);
                idx = lsv.IndexOf('/');
                Assert.Equal(3, idx);
                idx = lsv.IndexOfAny(new[] { '/' });
                Assert.Equal(3, idx);

                lsv = context.AllocateStringValue(null, lsv.Buffer, lsv.Size);

                idx = lsv.LastIndexOf('/');
                Assert.Equal(3, idx);
                idx = lsv.LastIndexOfAny(new[] { '/' });
                Assert.Equal(3, idx);
                idx = lsv.IndexOf('/');
                Assert.Equal(3, idx);
                idx = lsv.IndexOfAny(new[] { '/' });
                Assert.Equal(3, idx);
            }
        }
    }
}
