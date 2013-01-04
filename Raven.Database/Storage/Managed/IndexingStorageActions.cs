//-----------------------------------------------------------------------
// <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Exceptions;
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
		private readonly ReaderWriterLockSlim locker = new ReaderWriterLockSlim();

		public IndexingStorageActions(TableStorage storage)
		{
			this.storage = storage;
		}

		public IEnumerable<IndexStats> GetIndexesStats()
		{
			return from key in storage.IndexingStats.Keys
				   select storage.IndexingStats.Read(key)
					   into readResult
					   where readResult != null
					   select GetIndexStats(readResult);
		}

		public IndexStats GetIndexStats(string index)
		{
			locker.EnterReadLock();
			try
			{
				var readResult = storage.IndexingStats.Read(new RavenJObject { { "index", index } });
				if (readResult == null)
					return null;
				return GetIndexStats(readResult);
			}
			finally
			{
				locker.ExitReadLock();
			}

		}

		private static IndexStats GetIndexStats(Table.ReadResult readResult)
		{
			return new IndexStats
			{
				TouchCount = readResult.Key.Value<int>("touches"),
				IndexingAttempts = readResult.Key.Value<int>("attempts"),
				IndexingErrors = readResult.Key.Value<int>("failures"),
				IndexingSuccesses = readResult.Key.Value<int>("successes"),
				ReduceIndexingAttempts = readResult.Key.Value<int?>("reduce_attempts"),
				ReduceIndexingErrors = readResult.Key.Value<int?>("reduce_failures"),
				ReduceIndexingSuccesses = readResult.Key.Value<int?>("reduce_successes"),
				Name = readResult.Key.Value<string>("index"),
				LastIndexedEtag = new Guid(readResult.Key.Value<byte[]>("lastEtag")),
				LastIndexedTimestamp = readResult.Key.Value<DateTime>("lastTimestamp"),
				LastReducedEtag =
					readResult.Key.Value<byte[]>("lastReducedEtag") != null
						? (Guid?)new Guid(readResult.Key.Value<byte[]>("lastReducedEtag"))
						: null,
				LastReducedTimestamp = readResult.Key.Value<DateTime?>("lastReducedTimestamp")
			};
		}

		public void AddIndex(string name, bool createMapReduce)
		{
			var readResult = storage.IndexingStats.Read(name);
			if (readResult != null)
				throw new ArgumentException(string.Format("There is already an index with the name: '{0}'", name));

			storage.IndexingStats.UpdateKey(new RavenJObject
			{
				{"index", name},
				{"attempts", 0},
				{"successes", 0},
				{"failures", 0},
				{"touches", 0},
				{"lastEtag", Guid.Empty.ToByteArray()},
				{"lastTimestamp", DateTime.MinValue},
				
				{"reduce_attempts", createMapReduce? 0 : (RavenJToken)RavenJValue.Null},
				{"reduce_successes",createMapReduce? 0 : (RavenJToken)RavenJValue.Null},
				{"reduce_failures", createMapReduce? 0 : (RavenJToken)RavenJValue.Null},
				{"lastReducedEtag", createMapReduce? Guid.Empty.ToByteArray() : (RavenJToken)RavenJValue.Null},
				{"lastReducedTimestamp", createMapReduce? DateTime.MinValue : (RavenJToken)RavenJValue.Null}
			});
		}

		private RavenJObject GetCurrentIndex(string index)
		{
			var readResult = storage.IndexingStats.Read(index);
			if (readResult == null)
				throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));
			var key = (RavenJObject)readResult.Key;
			return key;
		}


		public void UpdateIndexingStats(string index, IndexingWorkStats stats)
		{
			var indexStats = (RavenJObject)GetCurrentIndex(index).CloneToken();
			indexStats["attempts"] = indexStats.Value<int>("attempts") + stats.IndexingAttempts;
			indexStats["successes"] = indexStats.Value<int>("successes") + stats.IndexingSuccesses;
			indexStats["failures"] = indexStats.Value<int>("failures") + stats.IndexingErrors;
			storage.IndexingStats.UpdateKey(indexStats);

		}

		public void UpdateReduceStats(string index, IndexingWorkStats stats)
		{
			var indexStats = GetCurrentIndex(index);
			indexStats["reduce_attempts"] = indexStats.Value<int>("reduce_attempts") + stats.ReduceAttempts;
			indexStats["reduce_successes"] = indexStats.Value<int>("reduce_successes") + stats.ReduceSuccesses;
			indexStats["reduce_failures"] = indexStats.Value<int>("reduce_failures") + stats.ReduceSuccesses;
			storage.IndexingStats.UpdateKey(indexStats);

		}

		public void RemoveAllDocumentReferencesFrom(string key)
		{
			foreach (var source in storage.DocumentReferences["ByKey"].SkipBefore(new RavenJObject { { "key", key } })
				.TakeWhile(x => key.Equals(x.Value<string>("key"), StringComparison.CurrentCultureIgnoreCase)))
			{
				storage.DocumentReferences.Remove(source);
			}
		}


		public void UpdateDocumentReferences(string view, string key, HashSet<string> references)
		{
			foreach (var source in storage.DocumentReferences["ByViewAndKey"].SkipBefore(new RavenJObject { { "view", view }, { "key", key } })
				.TakeWhile(x =>
					 view.Equals(x.Value<string>("view"), StringComparison.CurrentCultureIgnoreCase) &&
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
				.Distinct(StringComparer.InvariantCultureIgnoreCase);
		}

		public IEnumerable<string> GetDocumentsReferencesFrom(string key)
		{
			return storage.DocumentReferences["ByKey"].SkipTo(new RavenJObject { { "ref", key } })
				.TakeWhile(x => key.Equals(x.Value<string>("key"), StringComparison.CurrentCultureIgnoreCase))
				.Select(x => x.Value<string>("ref"))
				.Distinct(StringComparer.InvariantCultureIgnoreCase);
		}

		public void DeleteIndex(string name)
		{
			storage.IndexingStats.Remove(name);

			foreach (var table in new[] { storage.MappedResults, storage.ReduceResults, storage.ScheduleReductions, storage.ReduceKeys, storage.DocumentReferences })
			{
				foreach (var key in table["ByView"].SkipTo(new RavenJObject { { "view", name } })
					.TakeWhile(x => StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("view"), name)))
				{
					table.Remove(key);
				}
			}
		}

		public IndexFailureInformation GetFailureRate(string index)
		{
			var readResult = storage.IndexingStats.Read(index);
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
				Name = readResult.Key.Value<string>("index"),
			};
			return indexFailureInformation;
		}

		public void TouchIndexEtag(string index)
		{
			var readResult = storage.IndexingStats.Read(index);
			if (readResult == null)
				throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));
			var key = (RavenJObject)readResult.Key.CloneToken();
			key["touches"] = key.Value<int>("touches") + 1;
			storage.IndexingStats.UpdateKey(key);
		}

		public void UpdateLastIndexed(string index, Guid etag, DateTime timestamp)
		{
			locker.EnterWriteLock();
			try
			{
				bool updateOperationStatus = false;

				var sp = Stopwatch.StartNew();

				while (!updateOperationStatus)
				{
					var readResult = storage.IndexingStats.Read(index);
					if (readResult == null)
						throw new ArgumentException("There is no index with the name: " + index);

					var ravenJObject = (RavenJObject)readResult.Key.CloneToken();

					if (Buffers.Compare(ravenJObject.Value<byte[]>("lastEtag"), etag.ToByteArray()) >= 0)
					{
						break;
					}

					ravenJObject["lastEtag"] = etag.ToByteArray();
					ravenJObject["lastTimestamp"] = timestamp;

					updateOperationStatus = storage.IndexingStats.UpdateKey(ravenJObject);

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
			finally
			{
				locker.ExitWriteLock();
			}
		}

		public void UpdateLastReduced(string index, Guid etag, DateTime timestamp)
		{
			var readResult = storage.IndexingStats.Read(index);
			if (readResult == null)
				throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));

			var ravenJObject = (RavenJObject)readResult.Key.CloneToken();
			ravenJObject["lastReducedEtag"] = etag.ToByteArray();
			ravenJObject["lastReducedTimestamp"] = timestamp;

			storage.IndexingStats.UpdateKey(ravenJObject);
		}

		public void Dispose()
		{
		}
	}
}