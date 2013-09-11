// -----------------------------------------------------------------------
//  <copyright file="RavenDB_790.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_790 : RavenTest
	{
		private class Item { }

		[Fact]
		public void CanDisableQueryResultsTrackingForDocumentQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var items = session.Query<Item>().Customize(x => x.NoTracking().WaitForNonStaleResults()).ToList();
					
					Assert.Equal(2, items.Count);
					Assert.Equal(0, ((InMemoryDocumentSessionOperations) session).NumberOfEntitiesInUnitOfWork);
				}
			}
		}

		[Fact]
		public void CanDisableQueryResultsTrackingForAsyncDocumentQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				using (var asyncSession = store.OpenAsyncSession())
				{
					var asyncQuery = asyncSession.Query<Item>().Customize(x => x.NoTracking().WaitForNonStaleResults()).ToListAsync();

					asyncQuery.Wait();
					Assert.Equal(2, asyncQuery.Result.Count);
					Assert.Equal(0, ((InMemoryDocumentSessionOperations) asyncSession).NumberOfEntitiesInUnitOfWork);
				}
			}
		}

		[Fact]
		public void CanDisableQueryResultsTrackingForShardedDocumentQuery()
		{

			using (GetNewServer(8079))
			using (GetNewServer(8078))
			using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", CreateDocumentStore(8079)},
				{"2", CreateDocumentStore(8078)},
			})))
			{
				store.Initialize();

				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var items = session.Query<Item>().Customize(x => x.NoTracking().WaitForNonStaleResults()).ToList();

					Assert.Equal(2, items.Count);
					Assert.Equal(0, ((InMemoryDocumentSessionOperations) session).NumberOfEntitiesInUnitOfWork);
				}
			}
		}

		[Fact]
		public void CanDisableQueryResultsTrackingForAsyncShardedDocumentQuery()
		{
			using (GetNewServer(8079))
			using (GetNewServer(8078))
			using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", CreateDocumentStore(8079)},
				{"2", CreateDocumentStore(8078)},
			})))
			{
				store.Initialize();

				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				using (var asyncSession = store.OpenAsyncSession())
				{
					var asyncQuery = asyncSession.Query<Item>().Customize(x => x.NoTracking().WaitForNonStaleResults()).ToListAsync();

					asyncQuery.Wait();
					Assert.Equal(2, asyncQuery.Result.Count);
					Assert.Equal(0, ((InMemoryDocumentSessionOperations)asyncSession).NumberOfEntitiesInUnitOfWork);
				}
			}
		}

		[Fact]
		public void CanDisableQueryResultsCachingForDocumentQuery()
		{
			using (GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				store.JsonRequestFactory.ResetCache();

				using (var session = store.OpenSession())
				{
					var items = session.Query<Item>().Customize(x => x.NoCaching().WaitForNonStaleResults()).ToList();

					Assert.Equal(2, items.Count);
				}

				Assert.Equal(0, store.JsonRequestFactory.CurrentCacheSize);
			}
		}

		[Fact]
		public void CanDisableQueryResultsCachingForAsyncDocumentQuery()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				store.JsonRequestFactory.ResetCache();

				using (var asyncSession = store.OpenAsyncSession())
				{
					var asyncQuery = asyncSession.Query<Item>().Customize(x => x.NoCaching().WaitForNonStaleResults()).ToListAsync();

					asyncQuery.Wait();
					Assert.Equal(2, asyncQuery.Result.Count);
				}

				Assert.Equal(0, store.JsonRequestFactory.CurrentCacheSize);
			}
		}

		[Fact]
		public void CanDisableQueryResultsCachingForShardedDocumentQuery()
		{
			using (GetNewServer(8079))
			using (GetNewServer(8078))
			using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", CreateDocumentStore(8079)},
				{"2", CreateDocumentStore(8078)},
			})))
			{
				store.Initialize();

				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				store.ShardStrategy.Shards["1"].JsonRequestFactory.ResetCache();
				store.ShardStrategy.Shards["2"].JsonRequestFactory.ResetCache();

				using (var session = store.OpenSession())
				{
					var items = session.Query<Item>().Customize(x => x.NoCaching().WaitForNonStaleResults()).ToList();

					Assert.Equal(2, items.Count);
				}

				Assert.Equal(0, store.ShardStrategy.Shards["1"].JsonRequestFactory.CurrentCacheSize);
				Assert.Equal(0, store.ShardStrategy.Shards["2"].JsonRequestFactory.CurrentCacheSize);
			}
		}

		[Fact]
		public void CanDisableQueryResultsCachingForAsyncShardedDocumentQuery()
		{

			using (GetNewServer(8079))
			using (GetNewServer(8078))
			using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", CreateDocumentStore(8079)},
				{"2", CreateDocumentStore(8078)},
			})))
			{
				store.Initialize();

				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				store.ShardStrategy.Shards["1"].JsonRequestFactory.ResetCache();
				store.ShardStrategy.Shards["2"].JsonRequestFactory.ResetCache();

				using (var asyncSession = store.OpenAsyncSession())
				{
					var asyncQuery = asyncSession.Query<Item>().Customize(x => x.NoCaching().WaitForNonStaleResults()).ToListAsync();

					asyncQuery.Wait();
					Assert.Equal(2, asyncQuery.Result.Count);
				}

				Assert.Equal(0, store.ShardStrategy.Shards["1"].JsonRequestFactory.CurrentCacheSize);
				Assert.Equal(0, store.ShardStrategy.Shards["2"].JsonRequestFactory.CurrentCacheSize);
			}
		}

		[Fact]
		public void CanDisableLuceneQueryResultsTrackingForDocumentQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var items = session.Advanced.LuceneQuery<Item>().WaitForNonStaleResults().NoTracking().ToList();

					Assert.Equal(2, items.Count);
					Assert.Equal(0, ((InMemoryDocumentSessionOperations)session).NumberOfEntitiesInUnitOfWork);
				}
			}
		}

		[Fact]
		public void CanDisableLuceneQueryResultsCachingForDocumentQuery()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.Store(new Item());

					session.SaveChanges();
				}

				store.JsonRequestFactory.ResetCache();

				using (var session = store.OpenSession())
				{
					var items = session.Advanced.LuceneQuery<Item>().WaitForNonStaleResults().NoCaching().ToList();

					Assert.Equal(2, items.Count);
				}

				Assert.Equal(0, store.JsonRequestFactory.CurrentCacheSize);
			}
		}

		private static IDocumentStore CreateDocumentStore(int port)
		{
			return new DocumentStore
			{
				Url = string.Format("http://localhost:{0}/", port),
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			};
		}
	}
}