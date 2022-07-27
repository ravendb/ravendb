using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15909 : RavenTestBase
    {
        public RavenDB_15909(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Cached_Properties_Should_Renew_Correctly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var expandoObject = new ExpandoObject();
                    for (var i = 0; i < 520; i++)
                    {
                        expandoObject.TryAdd(i.ToString(), i);
                    }

                    await session.StoreAsync(expandoObject, "id1");
                    await session.StoreAsync(new object(), "id2");
                    await session.StoreAsync(new object(), "id3");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var count = 0;
                    var query = session.Advanced.AsyncRawQuery<dynamic>("from @all_docs");

                    var stream = await session.Advanced.StreamAsync(query);
                    while (await stream.MoveNextAsync())
                    {
                        count++;
                    }

                    Assert.Equal(3, count);
                }
            }
        }

        [Fact]
        public async Task Cached_Properties_Should_Renew_Correctly_Big_Stream()
        {
            using (var store = GetDocumentStore())
            {
                const int documentsCount = 8;
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < documentsCount; i++)
                    {
                        var expandoObject = new ExpandoObject();
                        for (var j = 0; j < CachedProperties.CachedPropertiesSize; j++)
                        {
                            expandoObject.TryAdd((i + j).ToString(), null);
                        }

                        await bulkInsert.StoreAsync(expandoObject, i.ToString());
                        expandoObject.Remove("Id", out _);
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    ((InMemoryDocumentSessionOperations)session)._maxDocsCountOnCachedRenewSession = 3;

                    var count = 0;
                    var query = session.Advanced.AsyncRawQuery<dynamic>("from @all_docs");

                    var stream = await session.Advanced.StreamAsync(query);
                    while (await stream.MoveNextAsync())
                    {
                        count++;
                    }

                    Assert.Equal(documentsCount, count);
                }
            }
        }
    }
}
