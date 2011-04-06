using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs.Async
{
	public class Querying : RemoteClientTest
    {
        [Fact]
        public void Can_query_using_async_session()
        {
            using(GetNewServer())
            using(var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                using (var s = store.OpenAsyncSession())
                {
                    s.Store(new {Name = "Ayende"});
                    s.SaveChangesAsync().Wait();
                }

                using(var s = store.OpenAsyncSession())
                {
                    var queryResultAsync = s.Advanced.AsyncLuceneQuery<dynamic>()
                        .WhereEquals("Name", "Ayende")
                        .ToListAsync();

                    queryResultAsync.Wait();

                    Assert.Equal("Ayende",
                        queryResultAsync.Result[0].Name);
                }
            }
        }
    }
}