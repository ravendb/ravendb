//-----------------------------------------------------------------------
// <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	using System.Diagnostics;
	using Abstractions.Extensions;

	public class IndexingStorageActions : IIndexingStorageActions
	{
		private readonly TableStorage storage;
		private readonly TimeSpan etagUpdateTimeout = TimeSpan.FromSeconds(2);
		
		public IndexingStorageActions(TableStorage storage)
		{
			this.storage = storage;
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			using (storage.ReadLock())
			{
				return (from key in storage.IndexingStats.Keys
				        let indexingStatsReadResult = storage.IndexingStats.Read(key)
				        where indexingStatsReadResult != null
				        let lastIndexedEtagReadResult = storage.LastIndexedEtags.Read(key)
				        select GetIndexStats(indexingStatsReadResult, lastIndexedEtagReadResult)).ToList();
			}
		}

		public IndexStats GetIndexStats(int index)
		{
			using (storage.ReadLock())
			{
				var readResult = storage.IndexingStats.Read(new RavenJObject { { "index", index.ToString() } });
				if (readResult == null)
					return null;
				Table.ReadResult lastIndexedEtagReadResult = storage.LastIndexedEtags.Read(new RavenJObject { { "index", index.ToString() } });
				return GetIndexStats(readResult, lastIndexedEtagReadResult);
			}
		}

		private static IndexStats GetIndexStats(Table.ReadResult indexingStatsResult, Table.ReadResult lastIndexedEtagsResult)
		{
			return new IndexStats
			{
				TouchCount = indexingStatsResult.Key.Value<int>("touches"),
				IndexingAttempts = indexingStatsResult.Key.Value<int>("attempts"),
				IndexingErrors = indexingStatsResult.Key.Value<int>("failures"),
				IndexingSuccesses = indexingStatsResult.Key.Value<int>("successes"),
				ReduceIndexingAttempts = indexingStatsResult.Key.Value<int?>("reduce_attempts"),
				ReduceIndexingErrors = indexingStatsResult.Key.Value<int?>("reduce_failures"),
				ReduceIndexingSuccesses = indexingStatsResult.Key.Value<int?>("reduce_successes"),
				Id = indexingStatsResult.Key.Value<int>("index"),
                Priority = (IndexingPriority)indexingStatsResult.Key.Value<int>("priority"),
				LastIndexedEtag = Etag.Parse(lastIndexedEtagsResult.Key.Value<byte[]>("lastEtag")),
				LastIndexedTimestamp = lastIndexedEtagsResult.Key.Value<DateTime>("lastTimestamp"),
                CreatedTimestamp = indexingStatsResult.Key.Value<DateTime>("createdTimestamp"),
				LastIndexingTime = indexingStatsResult.Key.Value<DateTime>("lastIndexingTime"),
				LastReducedEtag =
					indexingStatsResult.Key.Value<byte[]>("lastReducedEtag") != null
						? Etag.Parse(indexingStatsResult.Key.Value<byte[]>("lastReducedEtag"))
						: null,
				LastReducedTimestamp = indexingStatsResult.Key.Value<DateTime?>("lastReducedTimestamp")
			};
		}

		public void AddIndex(int name, bool createMapReduce)
		{
			using (storage.WriteLock())
			{
				var readResult = storage.IndexingStats.Read(name.ToString());
				if (readResult != null)
					throw new ArgumentException(string.Format("There is already an index with the name: '{0}'", name));

				storage.IndexingStats.UpdateKey(new RavenJObject
				{
					{"index", name},
					{"attempts", 0},
					{"successes", 0},
					{"failures", 0},
					{"priority", 1},
					{"touches", 0},
					{"createdTimestamp", SystemTime.UtcNow},
					{"lastIndexingTime", SystemTime.UtcNow},
					{"reduce_attempts", createMapReduce ? 0 : (RavenJToken) RavenJValue.Null},
					{"reduce_successes", createMapReduce ? 0 : (RavenJToken) RavenJValue.Null},
					{"reduce_failures", createMapReduce ? 0 : (RavenJToken) RavenJValue.Null},
					{"lastReducedEtag", createMapReduce ? Guid.Empty.ToByteArray() : (RavenJToken) RavenJValue.Null},
					{"lastReducedTimestamp", createMapReduce ? DateTime.MinValue : (RavenJToken) RavenJValue.Null}
				});

				storage.LastIndexedEtags.UpdateKey(new RavenJObject()
				{
					{"index", name},
					{"lastEtag", Guid.Empty.ToByteArray()},
					{"lastTimestamp", DateTime.MinValue},
				});
			}
		}

		private RavenJObject GetCurrentIndex(int index)
		{
			var readResult = storage.IndexingStats.Read(index.ToString());
			if (readResult == null)
				throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));
			var key = (RavenJObject)readResult.Key;
			return key;
		}


		public void UpdateIndexingStats(int index, IndexingWorkStats stats)
		{
			using (storage.WriteLock())
			{
				var indexStats = (RavenJObject) GetCurrentIndex(index).CloneToken();
				indexStats["attempts"] = indexStats.Value<int>("attempts") + stats.IndexingAttempts;
				indexStats["successes"] = indexStats.Value<int>("successes") + stats.IndexingSuccesses;
				indexStats["failures"] = indexStats.Value<int>("failures") + stats.IndexingErrors;
				indexStats["lastIndexingTime"] = SystemTime.UtcNow;
				storage.IndexingStats.UpdateKey(indexStats);
			}
		}

		public void UpdateReduceStats(int index, IndexingWorkStats stats)
		{
			using (storage.WriteLock())
			{
				var indexStats = GetCurrentIndex(index);
				indexStats["reduce_attempts"] = indexStats.Value<int>("reduce_attempts") + stats.ReduceAttempts;
				indexStats["reduce_successes"] = indexStats.Value<int>("reduce_successes") + stats.ReduceSuccesses;
				indexStats["reduce_failures"] = indexStats.Value<int>("reduce_failures") + stats.ReduceErrors;
				storage.IndexingStats.UpdateKey(indexStats);
			}
		}

		public void RemoveAllDocumentReferencesFrom(string key)
		{
			foreach (var source in storage.DocumentReferences["ByKey"].SkipBefore(new RavenJObject { { "key", key } })
				.TakeWhile(x => key.Equals(x.Value<string>("key"), StringComparison.CurrentCultureIgnoreCase)))
			{
				storage.DocumentReferences.Remove(source);
			}
		}


		public void UpdateDocumentReferences(int view, string key, HashSet<string> references)
		{
			foreach (var source in storage.DocumentReferences["ByViewAndKey"].SkipBefore(new RavenJObject { { "view", view }, { "key", key } })
				.TakeWhile(x =>
					 view.Equals(x.Value<int>("view")) &&
					 key.Equals(x.Value<string>("key"), StringComparison.CurrentCultureIgnoreCase)))
			{
				storage.DocumentReferences.Remove(source);
			}

			foreach (var reference in references)
			{
				storage.DocumentReferences.UpdateKey(new RavenJObject
				{
					{"view", view},
					{"key", key},
					{"ref", reference}
				});
			}
		}

		public IEnumerable<string> GetDocumentsReferencing(string key)
		{
			return storage.DocumentReferences["ByRef"].SkipTo(new RavenJObject { { "ref", key } })
				.TakeWhile(x => key.Equals(x.Value<string>("ref"), StringComparison.CurrentCultureIgnoreCase))
				.Select(x => x.Value<string>("key"))
				.Distinct(StringComparer.OrdinalIgnoreCase);
		}

		public int GetCountOfDocumentsReferencing(string key)
		{
			return storage.DocumentReferences["ByRef"].SkipTo(new RavenJObject {{"ref", key}})
			                                          .TakeWhile(
				                                          x =>
				                                          key.Equals(x.Value<string>("ref"),
				                                                     StringComparison.CurrentCultureIgnoreCase))
			                                          .Count();
		}

		public IEnumerable<string> GetDocumentsReferencesFrom(string key)
		{
			return storage.DocumentReferences["ByKey"].SkipTo(new RavenJObject { { "ref", key } })
				.TakeWhile(x => key.Equals(x.Value<string>("key"), StringComparison.CurrentCultureIgnoreCase))
				.Select(x => x.Value<string>("ref"))
				.Distinct(StringComparer.OrdinalIgnoreCase);
		}

	    public void PrepareIndexForDeletion(int id)
	    {
            storage.IndexingStats.Remove(id.ToString(CultureInfo.InvariantCulture));
            storage.LastIndexedEtags.Remove(id.ToString(CultureInfo.InvariantCulture));
	    }

	    public void DeleteIndex(int id, CancellationToken cancellationToken)
		{
			foreach (var table in new[] { storage.MappedResults, storage.ReduceResults, storage.ScheduleReductions, storage.ReduceKeys, storage.DocumentReferences })
			{
				foreach (var key in table["ByView"].SkipTo(new RavenJObject { { "view", id } })
					.TakeWhile(x => x.Value<int>("view") == id))
				{
				    if (cancellationToken.IsCancellationRequested)
				        return;
					table.Remove(key);
				}
			}
		}

	    public IndexFailureInformation GetFailureRate(int index)
		{
			var readResult = storage.IndexingStats.Read(index.ToString());
			if (readResult == null)
				throw new IndexDoesNotExistsException("There is no index named: " + index);
			var indexFailureInformation = new IndexFailureInformation
			{
				Attempts = readResult.Key.Value<int>("attempts"),
				Errors = readResult.Key.Value<int>("failures"),
				Successes = readResult.Key.Value<int>("successes"),
				ReduceAttempts = readResult.Key.Value<int?>("reduce_attempts"),
				ReduceErrors = readResult.Key.Value<int?>("reduce_failures"),
				ReduceSuccesses = readResult.Key.Value<int?>("reduce_successes"),
				Id = readResult.Key.Value<int>("index"),
			};
			return indexFailureInformation;
		}

		public void TouchIndexEtag(int index)
		{
			var readResult = storage.IndexingStats.Read(index.ToString());
			if (readResult == null)
				throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));
			var key = (RavenJObject)readResult.Key.CloneToken();
			key["touches"] = key.Value<int>("touches") + 1;
			storage.IndexingStats.UpdateKey(key);
		}

		public void SetIndexPriority(int index, IndexingPriority priority)
        {
            var readResult = storage.IndexingStats.Read(index.ToString());
            if (readResult == null)
                throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));
            var key = (RavenJObject)readResult.Key.CloneToken();
            key["priority"] = (int) priority;
            storage.IndexingStats.UpdateKey(key);
        }

		public void UpdateLastIndexed(int index, Etag etag, DateTime timestamp)
		{
			using (storage.WriteLock())
			{
				bool updateOperationStatus = false;

				var sp = Stopwatch.StartNew();

				while (!updateOperationStatus)
				{
					var readResult = storage.LastIndexedEtags.Read(index.ToString());
					if (readResult == null)
						throw new ArgumentException("There is no index with the name: " + index);

					var ravenJObject = (RavenJObject) readResult.Key.CloneToken();

					if (Buffers.Compare(ravenJObject.Value<byte[]>("lastEtag"), etag.ToByteArray()) >= 0)
					{
						break;
					}

					ravenJObject["lastEtag"] = etag.ToByteArray();
					ravenJObject["lastTimestamp"] = timestamp;

					updateOperationStatus = storage.LastIndexedEtags.UpdateKey(ravenJObject);

					if (!updateOperationStatus)
					{
						Thread.Sleep(100);
					}

					if (sp.Elapsed > etagUpdateTimeout)
					{
						break;
					}
				}
			}
		}

		public void UpdateLastReduced(int index, Etag etag, DateTime timestamp)
		{
			using (storage.WriteLock())
			{
				var readResult = storage.IndexingStats.Read(index.ToString());
				if (readResult == null)
					throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));

				var ravenJObject = (RavenJObject) readResult.Key.CloneToken();
				ravenJObject["lastReducedEtag"] = etag.ToByteArray();
				ravenJObject["lastReducedTimestamp"] = timestamp;

				storage.IndexingStats.UpdateKey(ravenJObject);
			}
		}

		public void Dispose()
		{
		}
	}
}
