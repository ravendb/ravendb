using System;
using System.Linq;

using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

using User = Raven.Tests.Bugs.User;

namespace Raven.SlowTests.Bugs
{
	public class VeryBigResultSetRemote : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/Esent/MaxVerPages"] = "512";
			configuration.Settings["Raven/Esent/PreferredVerPages"] = "512";
		}

		[Fact]
		public void CanGetVeryBigResultSetsEvenThoughItIsBadForYou()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 15000; i++)
					{
						session.Store(new User { });
					}
					session.SaveChanges();
				}

				server.SystemDatabase.Configuration.MaxPageSize = 20000;

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