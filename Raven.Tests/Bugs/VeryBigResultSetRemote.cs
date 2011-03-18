using System;
using System.Linq;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class VeryBigResultSetRemote : RemoteClientTest
	{
		[Fact]
		public void CanGetVeryBigResultSetsEvenThoughItIsBadForYou()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 15000; i++)
					{
						session.Store(new User { });
					}
					session.SaveChanges();
				}

				server.Database.Configuration.MaxPageSize = 20000;

				using (var session = store.OpenSession())
				{
					var users = session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults(TimeSpan.FromMinutes(1)))
						.Take(20000).ToArray();
					Assert.Equal(15000, users.Length);
				}
			}
		}
	}
}