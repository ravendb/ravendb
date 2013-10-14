using System.Threading.Tasks;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Core
{
	public class BasicSessionApiTests : RavenTestBase
	{
		[Fact]
		public async Task CanSaveAndLoad()
		{
			using (var store = new DocumentStore {Url = Url}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new User { Name = "Fitzchak" });
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.LoadAsync<User>("users/1");
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