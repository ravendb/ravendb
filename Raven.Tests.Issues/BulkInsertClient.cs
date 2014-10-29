using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class BulkInsertClient : RavenTest
	{
		[Fact]
		public void CanCreateAndDisposeUsingBulk()
		{
			using(var store = NewRemoteDocumentStore())
			{
				using(var bulkInsert = store.BulkInsert())
				{
					bulkInsert.Store(new User {Name = "Fitzchak"});
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					Assert.NotNull(user);
					Assert.Equal("Fitzchak", user.Name);
				}
			}
		}

		[Fact]
		public void CanCreateAndDisposeUsingBulk_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert())
				{
					bulkInsert.Store(new User { Name = "Fitzchak" });
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					Assert.NotNull(user);
					Assert.Equal("Fitzchak", user.Name);
				}
			}
		}

		private class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}