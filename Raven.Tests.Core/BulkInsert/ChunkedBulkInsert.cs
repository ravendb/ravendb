using Raven.Abstractions.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.BulkInsert
{
	public class ChunkedBulkInsert : RavenCoreTestBase
	{
		public class Node
		{
			public string Name { get; set; }
			public Node[] Children { get; set; }
		}

		[Fact]
		public void ChunkVolumeConstraint()
		{
			var bulkInsertStartsCounter = 0;
			using (var store = GetDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert(options:new Abstractions.Data.BulkInsertOptions{
				ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions{
					MaxChunkVolumeInBytes = 5
				}
				}))
				{
					store.Changes().ForBulkInsert(bulkInsert.OperationId).Subscribe(x =>
					{
						if (x.Type == DocumentChangeTypes.BulkInsertStarted)
							Interlocked.Increment(ref bulkInsertStartsCounter);
					});
					
					for (int i = 0; i < 20; i++)
					{
						bulkInsert.Store(new Node
								{
									Name = "Parent",
									Children = Enumerable.Range(0, 5).Select(x => new Node { Name = "Child" + x }).ToArray()
								}); 
					}
				}
				Assert.Equal(20, bulkInsertStartsCounter);
				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					var count = session.Query<Node>().Count();
					Assert.Equal(20,count);
				}
			}

		}

		[Fact]
		public void ChunkVolumeConstraintMakeSureUnneededConnectionsNotCreated()
		{
			var bulkInsertStartsCounter = 0;
			using (var store = GetDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert(options:new Abstractions.Data.BulkInsertOptions{
				ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions{
					MaxChunkVolumeInBytes = 10*1024
				}
				}))
				{
					store.Changes().ForBulkInsert(bulkInsert.OperationId).Subscribe(x =>
					{
						if (x.Type == DocumentChangeTypes.BulkInsertStarted)
							Interlocked.Increment(ref bulkInsertStartsCounter);
					});
					
					for (int i = 0; i < 20; i++)
					{
						bulkInsert.Store(new Node
								{
									Name = "Parent",
									Children = Enumerable.Range(0, 5).Select(x => new Node { Name = "Child" + x }).ToArray()
								}); 
					}
				}
				Assert.Equal(1, bulkInsertStartsCounter);
				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					var count = session.Query<Node>().Count();
					Assert.Equal(20,count);
				}
			}

		}

		[Fact]
		public void DocumentsInChunkConstraint()
		{
			var bulkInsertStartsCounter = 0;
			using (var store = GetDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert(options: new Abstractions.Data.BulkInsertOptions
				{
					ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
					{
						MaxDocumentsPerChunk = 1
					}
				}))
				{
					store.Changes().ForBulkInsert(bulkInsert.OperationId).Subscribe(x =>
					{
						if (x.Type == DocumentChangeTypes.BulkInsertStarted)
							Interlocked.Increment(ref bulkInsertStartsCounter);
					});

					for (int i = 0; i < 20; i++)
					{
						bulkInsert.Store(new Node
						{
							Name = "Parent",
						});
					}
				}

				Assert.Equal(20, bulkInsertStartsCounter);
				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					var count = session.Query<Node>().Count();
					Assert.Equal(count, 20);
				}
			}

		}

		[Fact]
		public void DocumentsInChunkConstraintMakeSureUnneedConnectionsNotCreated()
		{
			var bulkInsertStartsCounter = 0;
			using (var store = GetDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert(options: new Abstractions.Data.BulkInsertOptions
				{
					ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
					{
						MaxDocumentsPerChunk = 20
					}
				}))
				{
					store.Changes().ForBulkInsert(bulkInsert.OperationId).Subscribe(x =>
					{
						if (x.Type == DocumentChangeTypes.BulkInsertStarted)
							Interlocked.Increment(ref bulkInsertStartsCounter);
					});

					for (int i = 0; i < 20; i++)
					{
						bulkInsert.Store(new Node
						{
							Name = "Parent",
						});
					}
				}

				Assert.Equal(1, bulkInsertStartsCounter);
				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					var count = session.Query<Node>().Count();
					Assert.Equal(count, 20);
				}
			}
		}

		[Fact]
		public void ValidateChunkedBulkInsertOperationsIDsCount()
		{
			var bulkInsertStartsCounter = new ConcurrentDictionary<Guid, BulkInsertChangeNotification>();
			using (var store = GetDocumentStore())
			{
				for (var i=0; i<10; i++)
				{
					using (var bulkInsert = store.BulkInsert())
					{
						store.Changes().ForBulkInsert(bulkInsert.OperationId).Subscribe(x =>
						{
							if (x.Type == DocumentChangeTypes.BulkInsertStarted)
								Assert.True(bulkInsertStartsCounter.TryAdd(x.OperationId,x));
						});
						
						bulkInsert.Store(new Node
						{
							Name = "Parent",
							Children = Enumerable.Range(0, 5).Select(x => new Node {Name = "Child" + x}).ToArray()
						});
					}
				}
			}
			Assert.Equal(10, bulkInsertStartsCounter.Count);

		}
	}
}
