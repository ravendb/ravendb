using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Extensions;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Async
{
	public class Querying : RavenTest
	{
		[Fact]
		public async Task Can_query_using_async_session()
		{
			using(var store = NewRemoteDocumentStore())
			{
				using (var s = store.OpenAsyncSession())
				{
					await s.StoreAsync(new {Name = "Ayende"});
					await s.SaveChangesAsync();
				}

				using (var s = store.OpenAsyncSession())
				{
					var queryResultAsync = await s.Advanced.AsyncDocumentQuery<dynamic>()
						.WhereEquals("Name", "Ayende")
						.ToListAsync();

					var result = queryResultAsync;
					Assert.Equal("Ayende", result[0].Name);
				}
			}
		}
	}
}
