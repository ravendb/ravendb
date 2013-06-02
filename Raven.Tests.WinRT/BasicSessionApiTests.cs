using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class BasicSessionApiTests : RavenTestBase
	{
		[AssemblyInitialize]
		public static async Task SetUpData(TestContext context)
		{
			using (var store = new DocumentStore { Url = Url }.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new User { Name = "Fitzchak" });
					await session.StoreAsync(new User { Name = "Oren" });
					await session.SaveChangesAsync();
				}
			}
		}

		[AssemblyCleanup]
		public static async Task CleanupData()
		{
			using (var store = new DocumentStore { Url = Url }.Initialize())
			{
				await store.AsyncDatabaseCommands.DeleteByIndexAsync("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:Users"
				});
			}
		}

		[TestMethod]
		public async Task CanSaveAndLoad()
		{
			using (var store = new DocumentStore {Url = Url}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					var user = await session.LoadAsync<User>("users/1");
					Assert.IsNotNull(user);
					Assert.AreEqual("Fitzchak", user.Name);
				}
			}
		}

		[TestMethod]
		public async Task CanQueryCount()
		{
			using (var store = new DocumentStore { Url = Url }.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					var users = await session.Query<User>().ToListAsync();
					Assert.AreEqual(2, users.Count);
				}
			}
		}
		
		[TestMethod]
		public async Task CanUseCount()
		{
			using (var store = new DocumentStore { Url = Url }.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					var usersCount = await session.Query<User>().CountAsync();
					Assert.AreEqual(2, usersCount);
				}
			}
		}

		[TestMethod]
		public async Task CanUseAny()
		{
			using (var store = new DocumentStore { Url = Url }.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					bool condition = await session.Query<User>()
					                              .Where(user => user.IsActive)
					                              .AnyAsync();
					Assert.IsFalse(condition);
				}

				using (var session = store.OpenAsyncSession())
				{
					Assert.IsTrue(await session.Query<User>().AnyAsync());
				}
			}
		}

		private class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public bool IsActive { get; set; }
		}
	}
}