//-----------------------------------------------------------------------
// <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	public class IndexingStorageActions : IIndexingStorageActions
	{
		private readonly TableStorage storage;

		public IndexingStorageActions(TableStorage storage)
		{
			this.storage = storage;
		}


		public IEnumerable<IndexStats> GetIndexesStats()
		{
			foreach (var key in storage.IndexingStats.Keys)
			{
				var readResult = storage.IndexingStats.Read(key);
				if(readResult == null)
					continue;
				yield return new IndexStats
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
					LastReducedEtag = readResult.Key.Value<byte[]>("lastReducedEtag") != null ? (Guid?)new Guid(readResult.Key.Value<byte[]>("lastReducedEtag")) : null,
					LastReducedTimestamp = readResult.Key.Value<DateTime?>("lastReducedTimestamp")
				};
			}
		}

		public void AddIndex(string name, bool createMapReduce)
		{
			var readResult = storage.IndexingStats.Read(name);
			if(readResult != null)
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
			var indexStats = GetCurrentIndex(index);
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

		public void DeleteIndex(string name)
		{
			storage.IndexingStats.Remove(name);
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
			var key = (RavenJObject)readResult.Key;
			key["touches"] = key.Value<int>("touches") + 1;
			storage.IndexingStats.UpdateKey(key);
		}

		public void UpdateLastIndexed(string index, Guid etag, DateTime timestamp)
		{
			var readResult = storage.IndexingStats.Read(index);
			if (readResult == null)
				throw new ArgumentException("There is no index with the name: " + index);

			var ravenJObject = (RavenJObject)readResult.Key;
			ravenJObject["lastEtag"] = etag.ToByteArray();
			ravenJObject["lastTimestamp"] = timestamp;

			storage.IndexingStats.UpdateKey(ravenJObject);
		}

		public void UpdateLastReduced(string index, Guid etag, DateTime timestamp)
		{
			var readResult = storage.IndexingStats.Read(index);
			if (readResult == null)
				throw new ArgumentException(string.Format("There is no index with the name: '{0}'", index));

			var ravenJObject = (RavenJObject)readResult.Key;
			ravenJObject["lastReducedEtag"] = etag.ToByteArray();
			ravenJObject["lastReducedTimestamp"] = timestamp;

			storage.IndexingStats.UpdateKey(ravenJObject);
		}

		public void Dispose()
		{
		}
	}
}