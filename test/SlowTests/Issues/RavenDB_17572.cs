using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17572 : RavenTestBase
    {
        public RavenDB_17572(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task IndexWithGroupByOnDoubleShouldReturnDifferentHash()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Purchase
                    {
                        ItemType = "Tablet",
                        Date = 322.0
                    });
                    session.Store(new Purchase
                    {
                        ItemType = "Tablet",
                        Date = 123.0
                    });
                    session.SaveChanges();
                }

                new JavaScriptIndexWithGroupByOnDouble().Execute(store);
                new IndexWithGroupByOnDouble().Execute(store);
                Indexes.WaitForIndexing(store);

                var client = new HttpClient();
                var jsRes = await client.PostAsync($"{store.Urls[0]}/databases/{store.Database}/queries?debug=entries&addTimeSeriesNames=true&addSpatialProperties=true&metadataOnly=false&ignoreLimit=true", new StringContent($"{{\"Query\":\"from index 'JavaScriptIndexWithGroupByOnDouble'\",\"Start\":0,\"PageSize\":101,\"QueryParameters\":{{}}}}"));
                var jsonString1 = await jsRes.Content.ReadAsStringAsync();
                dynamic resultsObj1 = JsonConvert.DeserializeObject<ExpandoObject>(jsonString1);

                AssertQueryResults(resultsObj1);

                var cSharpRes = await client.PostAsync($"{store.Urls[0]}/databases/{store.Database}/queries?debug=entries&addTimeSeriesNames=true&addSpatialProperties=true&metadataOnly=false&ignoreLimit=true", new StringContent($"{{\"Query\":\"from index 'IndexWithGroupByOnDouble'\",\"Start\":0,\"PageSize\":101,\"QueryParameters\":{{}}}}"));
                var jsonString2 = await cSharpRes.Content.ReadAsStringAsync();
                dynamic resultsObj2 = JsonConvert.DeserializeObject<ExpandoObject>(jsonString2);

                AssertQueryResults(resultsObj2);
            }
        }

        private static void AssertQueryResults(dynamic resultsObj)
        {
            Assert.NotNull(resultsObj);
            Assert.Equal(2, resultsObj.TotalResults);
            List<dynamic> results = resultsObj.Results;
            Assert.Equal(2, results.Count);

            string hash1 = (string)((IDictionary<string, object>)results.First())["hash(key())"];
            string hash2 = (string)((IDictionary<string, object>)results.Last())["hash(key())"];
            Assert.NotEqual(hash1, hash2);
        }

        private class Purchase
        {
            public string ItemType;
            public double Date;
        }

        private class JavaScriptIndexWithGroupByOnDouble : AbstractIndexCreationTask
        {
            public override string IndexName => "JavaScriptIndexWithGroupByOnDouble";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"map(""Purchases"", (purchase) => {
    return {
        ItemType: purchase.ItemType,
        Date: purchase.Date
    };
})"
                    },
                    Reduce = @"groupBy(x => ({
    ItemType: x.ItemType,
    Date: x.Date
})).aggregate(g => {
        return {
                ItemType: x.ItemType,
                Date: x.Date
        };
    })"
                };
            }
        }

        private class IndexWithGroupByOnDouble : AbstractIndexCreationTask<Purchase>
        {
            public override string IndexName => "IndexWithGroupByOnDouble";
            public IndexWithGroupByOnDouble()
            {
                Map = docs => docs.Select(doc => new { ItemType = doc.ItemType, Date = (double)doc.Date });
                Reduce = results => results
                    .GroupBy(result => new { result.ItemType, result.Date })
                    .Select(result => new { result.Key.ItemType, Date = (double)result.Key.Date });
            }
        }
    }
}
