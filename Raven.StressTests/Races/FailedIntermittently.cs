using Raven.Tests.Bugs;
using Raven.Tests.Indexes;
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

        [Fact]
		public void WillNotProduceAnyErrors()
        {
			Run<MapReduceIndexOnLargeDataSet>(x => x.WillNotProduceAnyErrors(), 50);
        }
    }
}