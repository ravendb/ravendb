using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_8439 : NoDisposalNeeded
    {
        [Fact]
        public void NighlyBuildForceShouldBeSetToFalse()
        {
            Assert.False(NightlyBuildTheoryAttribute.Force);
        }
    }
}
