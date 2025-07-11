using Raven.Server.Documents.Indexes.Static;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_6960 : NoDisposalNeeded
    {
        public RavenDB_6960(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EnableDebuggingMustBeSetToFalseForSecurityReasons()
        {
            Assert.False(IndexCompiler.EnableDebugging);
        }
    }
}

