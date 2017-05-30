using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4918 : RavenTestBase
    {
        [Fact]
        public async Task CanGenerateCSharpDefintionForMultiMap()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new MultiMap());

                using (var client = new HttpClient())
                {
                    var url = $"{documentStore.Urls.First()}/databases/{documentStore.Database}/indexes/c-sharp-index-definition?name=MultiMap";
                    var response = await client.GetStringAsync(url);

                    Assert.Contains("from order in docs.Collection1", response);
                    Assert.Contains("from order in docs.Collection2", response);
                }
            }
        }

        private class MultiMap : AbstractIndexCreationTask
        {
            public override string IndexName => "MultiMap";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from order in docs.Collection1
select new { order.Company }",
                        @"from order in docs.Collection2
select new { order.Company }"
                    }
                };
            }
        }
    }
}