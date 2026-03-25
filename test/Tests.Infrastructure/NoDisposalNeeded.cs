using Tests.Infrastructure;
using Xunit;

namespace FastTests
{
    public abstract class NoDisposalNeeded(ITestOutputHelper output) : ParallelTestBase(output);
}
