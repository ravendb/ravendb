using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Dummy
	{
		public string Id { get; set; }
		public string Name { get; set; }
	}

	public class AsyncTest : RavenTest
	{
		[Fact]
		public void SyncQuery()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			using (var session = store.OpenSession())
			{
				var results = session.Query<Dummy>().ToList();
				Assert.Equal(0, results.Count);
				results = session.Query<Dummy>().ToList();
				Assert.Equal(0, results.Count);
			}
		}

		[Fact]
		public void AsyncQuery()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			using (var session = store.OpenAsyncSession())
			{
				var results = session.Query<Dummy>().ToListAsync();
				results.Wait();
				Assert.Equal(0, results.Result.Count);
				var results2 = session.Query<Dummy>().ToListAsync();
				results2.Wait();
				Assert.Equal(0, results2.Result.Count);
			}
		}

		[Fact]
		public void AsyncQuery_WithWhereClause()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					session.Store(new Dummy{Name = "oren"});
					session.SaveChangesAsync().Wait();
				}
				using (var session = store.OpenAsyncSession())
				{
					var results = session.Query<Dummy>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.Name == "oren")
						.ToListAsync();
					results.Wait();
					Assert.Equal(1, results.Result.Count);
				}
			}
		}

		[Fact]
		public void AsyncLoadNonExistant()
		{
			// load a non-existant entity
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			using (var session = store.OpenAsyncSession())
			{
				var loaded = session.LoadAsync<Dummy>("dummies/-1337");
				loaded.Wait();
				Assert.Null(loaded.Result);
			}
		}

		[Fact]
		public void AsyncLoad()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					session.Store(new Dummy());

					session.SaveChangesAsync().Wait();
				}
				using (var session = store.OpenAsyncSession())
				{
					session.LoadAsync<Dummy>("dummies/1").Wait();
					Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
				}
				using (var session = store.OpenAsyncSession())
				{
					session.LoadAsync<Dummy>("dummies/1").Wait();
					Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}
	}
}