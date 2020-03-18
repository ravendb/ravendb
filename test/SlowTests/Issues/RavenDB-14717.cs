using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14717 : RavenTestBase
    {
        public RavenDB_14717(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotSaveAnIndexWithBadPattern()
        {
            using (var store = GetDocumentStore())
            {
                const string pattern = "test/123456/x.Company";
                const string indexName = "Orders/ByCompany";
                var index = new IndexDefinition
                {
                    Name = indexName,
                    Maps = { @"from order in docs.Orders
                            select
                            new
                            {
                                order.Company,
                                Count = 1
                            }" },
                    Reduce = @"from result in results
group result by result.Company into g
select new
{
    Company = g.Key,
    Count = g.Sum(x=> x.Count)
}",
                    OutputReduceToCollection = "test",
                    PatternForOutputReduceToCollectionReferences = pattern
                };

                var putIndexesOperation = new PutIndexesOperation(index);
                var error = Assert.Throws<IndexInvalidException>(() => store.Maintenance.Send(putIndexesOperation));
                Assert.True(error.Message.Contains($"Provided pattern is not supported: {pattern}"));

                var indexDefinition = store.Maintenance.Send(new GetIndexOperation(indexName));
                Assert.Null(indexDefinition);
            }
        }
    }
}
