using Raven.Database.Prefetching;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class PrefetchingBug : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.MaxNumberOfItemsToIndexInSingleBatch = 2;
			configuration.MaxNumberOfItemsToReduceInSingleBatch = 2;
			configuration.InitialNumberOfItemsToIndexInSingleBatch = 2;
			configuration.InitialNumberOfItemsToReduceInSingleBatch = 2;
		}

		[Fact]
		public void ShouldNotSkipAnything()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new User
						{
							Active = true
						});
					}
					session.SaveChanges();
				}

				WaitForIndexing(store); // waiting until the in memory queue is drained

				using(var session = store.OpenSession())
				{
					var users = session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.Active)
						.ToList();
					Assert.Equal(10, users.Count);
				}
			}
		}

		[Fact]
		public void ShouldHandleUpdateWhenUpdatedDocsAreLoadedFromDisk()
		{
			using (var store = NewDocumentStore())
			{
				store.DocumentDatabase.WorkContext.StopIndexing(); // stop indexing to be able manually manage the prefetcher
				store.DocumentDatabase.WorkContext.Configuration.MaxNumberOfItemsToPreFetchForIndexing = 1;

				var putResult1 = store.DocumentDatabase.Put("key/1", null, new RavenJObject(), new RavenJObject(), null);
				var putResult2 = store.DocumentDatabase.Put("key/2", null, new RavenJObject(), new RavenJObject(), null);
				var putResult3 = store.DocumentDatabase.Put("key/2", null, new RavenJObject(), new RavenJObject(), null); // update

				var docs = store.DocumentDatabase.Prefetcher.GetPrefetchingBehavior(PrefetchingUser.Indexer, null).GetDocumentsBatchFrom(Raven.Abstractions.Data.Etag.Empty);

				Assert.Equal(2, docs.Count);
				Assert.Equal(putResult1.ETag, docs[0].Etag); // the document taken from memory

				Assert.Equal(putResult3.ETag, docs[1].Etag); // the updated doc loaded from disk because we limited MaxNumberOfItemsToIndexInSingleBatch
			}
		}

		[Fact]
		public void ShouldHandleUpdateWhenPrefetchingDocsIsDisabled()
		{
			using (var store = NewDocumentStore())
			{
				store.DocumentDatabase.WorkContext.Configuration.DisableDocumentPreFetchingForIndexing = true;
				store.DocumentDatabase.WorkContext.StopIndexing(); // stop indexing to be able manually manage the prefetcher

				var putResult1 = store.DocumentDatabase.Put("key/1", null, new RavenJObject(), new RavenJObject(), null);
				var putResult2 = store.DocumentDatabase.Put("key/2", null, new RavenJObject(), new RavenJObject(), null);
				var putResult3 = store.DocumentDatabase.Put("key/2", null, new RavenJObject(), new RavenJObject(), null); // update

				var docs = store.DocumentDatabase.Prefetcher.GetPrefetchingBehavior(PrefetchingUser.Indexer, null).GetDocumentsBatchFrom(Raven.Abstractions.Data.Etag.Empty);
				Assert.Equal(2, docs.Count);
				Assert.Equal(putResult1.ETag, docs[0].Etag);
				Assert.Equal(putResult3.ETag, docs[1].Etag);
			}
		}
	}
}