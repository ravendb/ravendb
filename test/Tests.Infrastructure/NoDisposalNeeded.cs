using Tests.Infrastructure;
using Xunit.Abstractions;

namespace FastTests
{
    public abstract class NoDisposalNeeded : ParallelTestBase
    {
        protected NoDisposalNeeded(ITestOutputHelper output) : base(output)
        {
        }
    }
}
