using Raven.Client;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10163 : FacetTestBase
    {
        public RavenDB_10163(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void SupportForFacetOnAllResults(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateCameraCostIndex(store);
                InsertCameraData(store, GetCameras(100));

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Camera>("CameraCost")
                        .AggregateBy(x => x
                            .AllResults()
                            .AverageOn(y => y.Cost)
                            .SumOn(y => y.Cost)
                            .MaxOn(y => y.Cost)
                            .MinOn(y => y.Cost))
                        .Execute();

                    Assert.Equal(1, results.Count);

                    var result = results[Constants.Documents.Querying.Facet.AllResults];

                    Assert.Equal(Constants.Documents.Querying.Facet.AllResults, result.Name);
                    Assert.Equal(1, result.Values.Count);

                    var value = result.Values[0];

                    Assert.Equal(100, value.Count);
                    Assert.True(value.Average > 0);
                    Assert.True(value.Sum > 0);
                    Assert.True(value.Min > 0);
                    Assert.True(value.Max > 0);
                }
            }
        }
    }
}
