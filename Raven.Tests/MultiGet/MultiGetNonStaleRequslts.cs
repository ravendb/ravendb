using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetNonStaleRequslts : RemoteClientTest
	{
		[Fact]
		public void ShouldBeAbleToGetNonStaleResults()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToList();
				}

				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}
				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.Lazily();

					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}
	}
}