using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Client.Document;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class BasicSessionApiTests : RavenTestBase
	{
		[TestMethod]
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
					Assert.IsNotNull(user);
					Assert.AreEqual("Fitzchak", user.Name);
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