// -----------------------------------------------------------------------
//  <copyright file="IndexingStorageActionsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading;

	using Microsoft.Isam.Esent.Interop;

	using Raven.Abstractions;
	using Raven.Abstractions.Data;
	using Raven.Database.Indexing;

	using Xunit;
	using Xunit.Extensions;

	using global::Voron.Exceptions;

	[Trait("VoronTest", "StorageActionsTests")]
	public class IndexingStorageActionsTests : TransactionalStorageTestBase
	{
	    [Theory]
	    [PropertyData("Storages")]
	    public void IndexCreation_In_DifferentBatches(string requestedStorage)
	    {
	        using (var storage = NewTransactionalStorage(requestedStorage))
	        {
                storage.Batch(accessor => accessor.Indexing.AddIndex(101, false));
                
                //make sure that index already exists check works correctly
                Assert.DoesNotThrow(() => storage.Batch(accessor => accessor.Indexing.AddIndex(202, false)));
            }
	    }

	    [Theory]
		[PropertyData("Storages")]
		public void IndexCreation(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor =>
				{
					accessor.Indexing.AddIndex(101, false);
					accessor.Indexing.AddIndex(202, true);
				});

				storage.Batch(accessor =>
				{
					var stats = accessor.Indexing.GetIndexesStats().ToList();
					Assert.Equal(2, stats.Count);

					var stat1 = stats[0];
					Assert.Equal(101, stat1.Id);
					Assert.Equal(0, stat1.IndexingAttempts);
					Assert.Equal(0, stat1.IndexingSuccesses);
					Assert.Equal(0, stat1.IndexingErrors);
					Assert.Equal(IndexingPriority.Normal, stat1.Priority);
					Assert.Equal(0, stat1.TouchCount);
                    Assert.Equal(UtcNow, stat1.CreatedTimestamp);
					Assert.Equal(DateTime.MinValue, stat1.LastIndexingTime);
					Assert.Null(stat1.ReduceIndexingAttempts);
					Assert.Null(stat1.ReduceIndexingSuccesses);
					Assert.Null(stat1.ReduceIndexingErrors);
					Assert.Null(stat1.LastReducedEtag);
					Assert.Null(stat1.LastReducedTimestamp);

					Assert.Equal(Etag.Empty, stat1.LastIndexedEtag);
					Assert.Equal(DateTime.MinValue, stat1.LastIndexedTimestamp);

					var stat2 = stats[1];
					Assert.Equal(202, stat2.Id);
					Assert.Equal(0, stat2.IndexingAttempts);
					Assert.Equal(0, stat2.IndexingSuccesses);
					Assert.Equal(0, stat2.IndexingErrors);
					Assert.Equal(IndexingPriority.Normal, stat2.Priority);
					Assert.Equal(0, stat2.TouchCount);
                    Assert.Equal(UtcNow, stat2.CreatedTimestamp);
					Assert.Equal(DateTime.MinValue, stat2.LastIndexingTime);
					Assert.Equal(0, stat2.ReduceIndexingAttempts);
					Assert.Equal(0, stat2.ReduceIndexingSuccesses);
					Assert.Equal(0, stat2.ReduceIndexingErrors);
					Assert.Equal(Etag.Empty, stat2.LastReducedEtag);
					Assert.Equal(DateTime.MinValue, stat2.LastReducedTimestamp);

					Assert.Equal(Etag.Empty, stat2.LastIndexedEtag);
					Assert.Equal(DateTime.MinValue, stat2.LastIndexedTimestamp);
				});

				storage.Batch(accessor =>
				{
					accessor.Indexing.PrepareIndexForDeletion(101);
					accessor.Indexing.PrepareIndexForDeletion(202);

					accessor.Indexing.DeleteIndex(101, new CancellationToken());
					accessor.Indexing.DeleteIndex(202, new CancellationToken());
				});

				storage.Batch(accessor =>
				{
					var stats = accessor.Indexing.GetIndexesStats().ToList();
					Assert.Equal(0, stats.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void CannotAddDuplicateIndex(string requestedStorage)
		{
			Type exception1Type = null;
			Type exception2Type = null;
			string exception1Message = null;
			string exception2Message = null;
			if (requestedStorage == "esent")
			{
				exception1Type = typeof(EsentKeyDuplicateException);
				exception1Message = "Illegal duplicate key";

				exception2Type = typeof(EsentKeyDuplicateException);
				exception2Message = "Illegal duplicate key";
			}
			else if (requestedStorage == "voron")
			{
				exception1Type = typeof(ArgumentException);
				exception1Message = "There is already an index with the name: '101'";

				exception2Type = typeof(ArgumentException);
				exception2Message = "There is already an index with the name: '202'";
			}

			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, false));

				var e1 = Assert.Throws(exception1Type, () => storage.Batch(accessor => accessor.Indexing.AddIndex(101, false)));

				Assert.Equal(exception1Message, e1.Message);

				var e2 = Assert.Throws(exception2Type,
					() => storage.Batch(
						accessor =>
						{
							accessor.Indexing.AddIndex(202, false);
							accessor.Indexing.AddIndex(202, true);
						}));

				Assert.Equal(exception2Message, e2.Message);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IndexStats(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(
					accessor =>
					{
						accessor.Indexing.AddIndex(101, false);
						accessor.Indexing.AddIndex(202, true);
					});

				storage.Batch(accessor =>
				{
					var stats = accessor.Indexing.GetIndexesStats().ToList();
					Assert.Equal(2, stats.Count);

					var stat1 = accessor.Indexing.GetIndexStats(101);
					Assert.Equal(101, stat1.Id);
					Assert.Equal(0, stat1.IndexingAttempts);
					Assert.Equal(0, stat1.IndexingSuccesses);
					Assert.Equal(0, stat1.IndexingErrors);
					Assert.Equal(IndexingPriority.Normal, stat1.Priority);
					Assert.Equal(0, stat1.TouchCount);
                    Assert.Equal(UtcNow, stat1.CreatedTimestamp);
					Assert.Equal(DateTime.MinValue, stat1.LastIndexingTime);
					Assert.Null(stat1.ReduceIndexingAttempts);
					Assert.Null(stat1.ReduceIndexingSuccesses);
					Assert.Null(stat1.ReduceIndexingErrors);
					Assert.Null(stat1.LastReducedEtag);
					Assert.Null(stat1.LastReducedTimestamp);

					Assert.Equal(Etag.Empty, stat1.LastIndexedEtag);
					Assert.Equal(DateTime.MinValue, stat1.LastIndexedTimestamp);

					var stat2 = accessor.Indexing.GetIndexStats(202);
					Assert.Equal(202, stat2.Id);
					Assert.Equal(0, stat2.IndexingAttempts);
					Assert.Equal(0, stat2.IndexingSuccesses);
					Assert.Equal(0, stat2.IndexingErrors);
					Assert.Equal(IndexingPriority.Normal, stat2.Priority);
					Assert.Equal(0, stat2.TouchCount);
                    Assert.Equal(UtcNow, stat2.CreatedTimestamp);
					Assert.Equal(DateTime.MinValue, stat2.LastIndexingTime);
					Assert.Equal(0, stat2.ReduceIndexingAttempts);
					Assert.Equal(0, stat2.ReduceIndexingSuccesses);
					Assert.Equal(0, stat2.ReduceIndexingErrors);
					Assert.Equal(Etag.Empty, stat2.LastReducedEtag);
					Assert.Equal(DateTime.MinValue, stat2.LastReducedTimestamp);

					Assert.Equal(Etag.Empty, stat2.LastIndexedEtag);
					Assert.Equal(DateTime.MinValue, stat2.LastIndexedTimestamp);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void IndexPriority(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, false));

				storage.Batch(
					accessor =>
					{
						var stats = accessor.Indexing.GetIndexStats(101);
						Assert.Equal(IndexingPriority.Normal, stats.Priority);

						accessor.Indexing.SetIndexPriority(101, IndexingPriority.Forced);
					});

				storage.Batch(
					accessor =>
					{
						var stats = accessor.Indexing.GetIndexStats(101);
						Assert.Equal(IndexingPriority.Forced, stats.Priority);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void FailureRate(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(
					accessor =>
					{
						accessor.Indexing.AddIndex(101, false);
						accessor.Indexing.AddIndex(202, true);
					});

				storage.Batch(accessor =>
				{
					var rate1 = accessor.Indexing.GetFailureRate(101);
					Assert.NotNull(rate1);
					Assert.Equal(0, rate1.Attempts);
					Assert.Equal(0, rate1.Errors);
					Assert.Equal(0, rate1.Successes);
					Assert.Null(rate1.ReduceAttempts);
					Assert.Null(rate1.ReduceErrors);
					Assert.Null(rate1.ReduceSuccesses);

					var rate2 = accessor.Indexing.GetFailureRate(202);
					Assert.NotNull(rate2);
					Assert.Equal(0, rate2.Attempts);
					Assert.Equal(0, rate2.Errors);
					Assert.Equal(0, rate2.Successes);
					Assert.Equal(0, rate2.ReduceAttempts);
					Assert.Equal(0, rate2.ReduceErrors);
					Assert.Equal(0, rate2.ReduceSuccesses);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void UpdateLastIndexed(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, false));

				var etag = new Etag(Guid.NewGuid().ToString());
				var date = DateTime.Now.AddDays(1);

				storage.Batch(accessor => accessor.Indexing.UpdateLastIndexed(101, etag, date));

				storage.Batch(accessor =>
				{
					var stat1 = accessor.Indexing.GetIndexStats(101);
					Assert.Equal(101, stat1.Id);
					Assert.Equal(etag, stat1.LastIndexedEtag);
					Assert.Equal(date, stat1.LastIndexedTimestamp);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void UpdateLastReduced(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, true));

				var etag = new Etag(Guid.NewGuid().ToString());
				var date = DateTime.Now.AddDays(1);

				storage.Batch(accessor => accessor.Indexing.UpdateLastReduced(101, etag, date));

				storage.Batch(accessor =>
				{
					var stat1 = accessor.Indexing.GetIndexStats(101);
					Assert.Equal(101, stat1.Id);
					Assert.Equal(etag, stat1.LastReducedEtag);
					Assert.Equal(date, stat1.LastReducedTimestamp);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void TouchIndexEtag(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, true));

				storage.Batch(accessor =>
				{
					var stat1 = accessor.Indexing.GetIndexStats(101);
					Assert.Equal(101, stat1.Id);
					Assert.Equal(0, stat1.TouchCount);
				});

				storage.Batch(accessor => accessor.Indexing.TouchIndexEtag(101));

				storage.Batch(accessor =>
				{
					var stat1 = accessor.Indexing.GetIndexStats(101);
					Assert.Equal(101, stat1.Id);
					Assert.Equal(1, stat1.TouchCount);
				});

				storage.Batch(accessor => accessor.Indexing.TouchIndexEtag(101));

				storage.Batch(accessor =>
				{
					var stat1 = accessor.Indexing.GetIndexStats(101);
					Assert.Equal(101, stat1.Id);
					Assert.Equal(2, stat1.TouchCount);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void UpdateIndexingStats(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, true));

				storage.Batch(accessor => accessor.Indexing.UpdateIndexingStats(101, new IndexingWorkStats
																						  {
																							  IndexingAttempts = 11,
																							  IndexingErrors = 3,
																							  IndexingSuccesses = 2,
																							  Operation = IndexingWorkStats.Status.Reduce,
																							  ReduceAttempts = 6,
																							  ReduceErrors = 7,
																							  ReduceSuccesses = 9
																						  }));

				storage.Batch(accessor =>
				{
					var stat = accessor.Indexing.GetIndexStats(101);
					Assert.NotNull(stat);
					Assert.Equal(11, stat.IndexingAttempts);
					Assert.Equal(3, stat.IndexingErrors);
					Assert.Equal(2, stat.IndexingSuccesses);
                    Assert.Equal(UtcNow, stat.LastIndexingTime);
					Assert.Equal(0, stat.ReduceIndexingAttempts);
					Assert.Equal(0, stat.ReduceIndexingErrors);
					Assert.Equal(0, stat.ReduceIndexingSuccesses);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void UpdateReduceStats(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, true));

				storage.Batch(accessor => accessor.Indexing.UpdateReduceStats(101, new IndexingWorkStats
				{
					IndexingAttempts = 11,
					IndexingErrors = 3,
					IndexingSuccesses = 2,
					Operation = IndexingWorkStats.Status.Reduce,
					ReduceAttempts = 6,
					ReduceErrors = 7,
					ReduceSuccesses = 9
				}));

				storage.Batch(accessor =>
				{
					var stat = accessor.Indexing.GetIndexStats(101);
					Assert.NotNull(stat);
					Assert.Equal(0, stat.IndexingAttempts);
					Assert.Equal(0, stat.IndexingErrors);
					Assert.Equal(0, stat.IndexingSuccesses);
					Assert.Equal(6, stat.ReduceIndexingAttempts);
					Assert.Equal(7, stat.ReduceIndexingErrors);
					Assert.Equal(9, stat.ReduceIndexingSuccesses);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentReferences1(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, true));

				storage.Batch(accessor => accessor.Indexing.UpdateDocumentReferences(303, "key1", new HashSet<string>()));

				storage.Batch(accessor =>
				{
					var references = accessor.Indexing.GetDocumentsReferencesFrom("key1").ToList();
					Assert.Equal(0, references.Count);
				});

				storage.Batch(accessor => accessor.Indexing.UpdateDocumentReferences(303, "key1", new HashSet<string> { "key2", "key3" }));

				storage.Batch(accessor =>
				{
					var references = accessor.Indexing.GetDocumentsReferencesFrom("key1").ToList();
					Assert.Equal(2, references.Count);

					Assert.True(references.Contains("key2"));
					Assert.True(references.Contains("key3"));
				});

				storage.Batch(accessor => accessor.Indexing.RemoveAllDocumentReferencesFrom("key1"));

				storage.Batch(accessor =>
				{
					var references = accessor.Indexing.GetDocumentsReferencesFrom("key1").ToList();
					Assert.Equal(0, references.Count);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentReferences2(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor => accessor.Indexing.AddIndex(101, true));

				storage.Batch(accessor => accessor.Indexing.UpdateDocumentReferences(303, "key1", new HashSet<string> { "key2", "key3" }));
				storage.Batch(accessor => accessor.Indexing.UpdateDocumentReferences(303, "key2", new HashSet<string> { "key1", "key3" }));

				storage.Batch(accessor =>
				{
					var documents = accessor.Indexing.GetDocumentsReferencing("key3").ToList();
					Assert.Equal(2, documents.Count);

					Assert.True(documents.Contains("key1"));
					Assert.True(documents.Contains("key2"));

					Assert.Equal(documents.Count, accessor.Indexing.GetCountOfDocumentsReferencing("key3"));
					Assert.Equal(1, accessor.Indexing.GetCountOfDocumentsReferencing("key1"));
					Assert.Equal(1, accessor.Indexing.GetCountOfDocumentsReferencing("key2"));
				});
			}
		}
	}
}