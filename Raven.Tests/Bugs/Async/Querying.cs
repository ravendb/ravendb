using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Bugs.Async
{
	public class Querying : RemoteClientTest
	{
		[Fact]
		public void Can_query_using_async_session()
		{
			using(var store = NewRemoteDocumentStore())
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

					var result = queryResultAsync.Result;
					Assert.Equal("Ayende",
						result[0].Name);
				}
			}
		}
	}
}