using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_8439(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Codebase)]
        public void NighlyBuildForceShouldBeSetToFalse()
        {
            Assert.False(NightlyBuildTheoryAttribute.Force);
        }
    }
}

