using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8026 : FacetTestBase
    {
        [Fact]
        public void OptionsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateCameraCostIndex(store);

                InsertCameraData(store, GetCameras(100));

                using (var session = store.OpenSession())
                {
                    var result = session.Query<Camera, CameraCostIndex>()
                        .AggregateBy(x => x.ByField(y => y.Manufacturer).WithOptions(new FacetOptions
                        {
                            TermSortMode = FacetTermSortMode.CountDesc
                        }))
                        .Execute();

                    var counts = result["Manufacturer"].Values.Select(x => x.Count).ToList();

                    var orderedCounts = result["Manufacturer"].Values
                        .Select(x => x.Count)
                        .OrderByDescending(x => x)
                        .ToList();

                    Assert.Equal(counts, orderedCounts);
                }
            }
        }
    }
}
