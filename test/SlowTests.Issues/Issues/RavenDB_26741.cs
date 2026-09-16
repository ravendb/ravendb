using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26741 : RavenTestBase
{
    public RavenDB_26741(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void DisableSparseRegionsCannotBeSetPerIndex()
    {
        using (var store = GetDocumentStore())
        {
            var index = new IndexWithDisabledSparseRegions();

            var exception = Assert.Throws<IndexCreationException>(() => index.Execute(store));

            Assert.Contains($"Could not create index '{index.IndexName}' because the configuration option key 'Storage.DisableSparseRegions' is not recognized", exception.Message);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
    }

    private class IndexWithDisabledSparseRegions : AbstractIndexCreationTask<Dto>
    {
        public IndexWithDisabledSparseRegions()
        {
            Map = dtos => from dto in dtos
                select new { dto.Id };

            Configuration = new IndexConfiguration { { "Storage.DisableSparseRegions", "true" } };
        }
    }
}
