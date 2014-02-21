extern alias client;
using client::Raven.Bundles.Authorization.Model;
using Raven.Abstractions.Data;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Bundles.Tests.Authorization.Bugs
{
	public class Kwal : AuthorizationTest
	{
		[Fact]
		public void WillAbortDeleteIfUserDoesNotHavePermissions()
		{
			using (var session = store.OpenSession())
			{
				session.Store(
					new DatabaseDocument()
					{
						Id = "Raven/Databases/Testing",
						Settings =
						   {
							   { "Raven/RunInMemory", "false" },
							   { "Raven/DataDir", "~\\Testing" }
						   }
					}
				);

				session.SaveChanges();

				store.DatabaseCommands.EnsureDatabaseExists("Testing");
			}

			using (var session = store.OpenSession("Testing"))
			{
				session.Store(
					new AuthorizationUser()
					{
						Id = "Authorization/Users/Johnny",
						Name = "Johnny Executive"
					}
				);

				session.SaveChanges();
			}
		}
	}
}