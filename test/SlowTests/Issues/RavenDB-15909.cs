using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using FastTests;
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
    }
}
