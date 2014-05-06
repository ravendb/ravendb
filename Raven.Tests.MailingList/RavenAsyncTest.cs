using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class AsyncTest : RavenTest
	{
		public class Dummy
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void SyncQuery()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			using (var session = store.OpenSession())
			{
				var results = session.Query<Dummy>().ToList();
				Assert.Equal(0, results.Count);
				results = session.Query<Dummy>().ToList();
				Assert.Equal(0, results.Count);
			}
		}

		[Fact]
		public async Task AsyncQuery()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			using (var session = store.OpenAsyncSession())
			{
				var results = await session.Query<Dummy>().ToListAsync();
				Assert.Equal(0, results.Count);
				var results2 = await session.Query<Dummy>().ToListAsync();
				Assert.Equal(0, results2.Count);
			}
		}

		[Fact]
		public async Task AsyncQuery_WithWhereClause()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new Dummy {Name = "oren"});
					await session.SaveChangesAsync();
				}
				using (var session = store.OpenAsyncSession())
				{
					var results = await session.Query<Dummy>()
					                           .Customize(x => x.WaitForNonStaleResults())
					                           .Where(x => x.Name == "oren")
					                           .ToListAsync();
					Assert.Equal(1, results.Count);
				}
			}
		}

		[Fact]
		public async Task AsyncLoadNonExistant()
		{
			// load a non-existant entity
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			using (var session = store.OpenAsyncSession())
			{
				var loaded = await session.LoadAsync<Dummy>("dummies/-1337");
				Assert.Null(loaded);
			}
		}

		[Fact]
		public async Task AsyncLoad()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new Dummy());
					await session.SaveChangesAsync();
				}
				using (var session = store.OpenAsyncSession())
				{
					await session.LoadAsync<Dummy>("dummies/1");
					Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
				}
				using (var session = store.OpenAsyncSession())
				{
					await session.LoadAsync<Dummy>("dummies/1");
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}
	}
}