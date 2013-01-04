using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DeletingDynamics : RavenTest
	{
		[Fact]
		public void CanDeleteItemsUsingDynamic()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("users/1", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/2", null, new RavenJObject(), new RavenJObject());

				using (var session = store.OpenSession())
				{
					var user1 = session.Load<dynamic>("users/1");
					var user2 = session.Load<dynamic>("users/2");

					session.Delete(user1);
					session.Delete(user2);

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user1 = session.Load<dynamic>("users/1");
					var user2 = session.Load<dynamic>("users/2");

					Assert.Null(user1);
					Assert.Null(user2);
				}
			}
		}
	}
}
