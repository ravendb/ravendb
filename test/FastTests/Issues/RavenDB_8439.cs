using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_8439 : NoDisposalNeeded
    {
        public RavenDB_8439(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void NighlyBuildForceShouldBeSetToFalse()
        {
            Assert.False(NightlyBuildTheoryAttribute.Force);
        }
    }
}
