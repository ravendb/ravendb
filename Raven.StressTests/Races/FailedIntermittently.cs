using Raven.Tests.Bugs;
using Xunit;

namespace Raven.StressTests.Races
{
    public class FailedIntermittently : StressTest
    {
        [Fact]
        public void AfterDeletingAndStoringTheDocumentIsIndexed()
        {
            Run<IndexingBehavior>(x => x.AfterDeletingAndStoringTheDocumentIsIndexed(), 20);
        }
    }
}