using Raven.Server.Documents.Indexes.Static;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6960 : NoDisposalNeeded
    {
        [Fact]
        public void EnableDebuggingMustBeSetToFalseForSecurityReasons()
        {
            Assert.False(IndexCompiler.EnableDebugging);
        }
    }
}
