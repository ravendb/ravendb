using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Json.Linq;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;

using Sparrow;

using Xunit;

namespace FastTests.Client.Indexing
{
    public class DebugIndexing : RavenTestBase
    {
        private class Person
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task QueriesRunning()
        {
            using (var store = await GetDocumentStore())
            {
                IndexQuery query;
                using (var session = store.OpenSession())
                {
                    var people = session.Query<Person>()
                        .Where(x => x.Name == "John")
                        .ToList(); // create index

                    query = session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(x => x.Name, "John")
                        .GetIndexQuery(isAsync: false);
                }

                var database = await Server
                    .ServerStore
                    .DatabasesLandlord
                    .TryGetOrCreateResourceStore(new StringSegment(store.DefaultDatabase, 0));

                var index = database.IndexStore.GetIndex(1);

                var now = SystemTime.UtcNow;
                index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query, 10, OperationCancelToken.None));

                string jsonString;
                using (var client = new HttpClient())
                {
                    jsonString = await client.GetStringAsync($"{store.Url.ForDatabase(store.DefaultDatabase)}/debug/queries/running");
                }

                var json = RavenJObject.Parse(jsonString);
                var array = json.Value<RavenJArray>(index.Name);

                Assert.Equal(1, array.Length);

                var info = array[0].JsonDeserialization<ExecutingQueryInfo>();

                Assert.NotNull(array[0].Value<string>(nameof(ExecutingQueryInfo.Duration)));
                Assert.Equal(10, info.QueryId);
                Assert.Equal(now, info.StartTime);
                Assert.Null(info.Token);
                Assert.Equal(query, info.QueryInfo);
            }
        }
    }
}