using Raven.Client.Document;
using Raven.Tests.Linq;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.MultiGet
{
	public class MultiGetQueries : RemoteClientTest
	{
		[Fact(Skip = "Not impl")]
		public void UnlessAccessedLazyQueriesAreNoOp()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.Equal(0, session.Advanced.NumberOfRequests);
				}

			}
		}
	}
}