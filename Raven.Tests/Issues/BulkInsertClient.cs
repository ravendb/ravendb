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
				using(var bulkInsert = store.StartBulkInsert())
				{
					bulkInsert.Add(new User {Name = "Fitzchak"});
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