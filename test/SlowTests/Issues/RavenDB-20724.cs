using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20724 : RavenTestBase
{
    public RavenDB_20724(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void CheckIfUnrecognizedIndexConfigurationKeyThrowsException()
    {
        using (var store = GetDocumentStore())
        {
            var index = new DummyIndex();
            
            var exception = Assert.Throws<IndexCreationException>(() => index.Execute(store));

            Assert.Contains($"Could not create index '{index.IndexName}' because the configuration option key 'blabla1' is not recognized", exception.Message);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new { DummyValue = 1 };

            Configuration = new IndexConfiguration(){ { "Indexing.Metrics.Enabled", "true" }, { "blabla1", "blabla2" } };
        }
    }
}
