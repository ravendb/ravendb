//-----------------------------------------------------------------------
// <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class IndexingStorageActions : IIndexingStorageActions
    {
        private readonly TableStorage storage;

        private readonly ThreadLocal<string> currentIndex = new ThreadLocal<string>();

        public IndexingStorageActions(TableStorage storage)
        {
            this.storage = storage;
        }

        public void SetCurrentIndexStatsTo(string index)
        {
            currentIndex.Value = index;
        }

        public void IncrementIndexingAttempt()
        {
            var index = GetCurrentIndex();
            index["attempts"] = index.Value<int>("attempts") + 1;
            storage.IndexingStats.UpdateKey(index);
        }

		public void IncrementReduceIndexingAttempt()
		{
			var index = GetCurrentIndex();
			index["reduce_attempts"] = index.Value<int>("reduce_attempts") + 1;
			storage.IndexingStats.UpdateKey(index);
		}

        private RavenJObject GetCurrentIndex()
        {
            var readResult = storage.IndexingStats.Read(currentIndex.Value);
            if (readResult == null)
                throw new ArgumentException("There is no index with the name: " + currentIndex.Value);
        	var key = (RavenJObject)readResult.Key;
        	return key;
        }

        public void IncrementSuccessIndexing()
        {
            var index = GetCurrentIndex();
            index["successes"] = index.Value<int>("successes") + 1;
            storage.IndexingStats.UpdateKey(index);
        }

        public void IncrementIndexingFailure()
        {
            var index = GetCurrentIndex();
            index["failures"] = index.Value<int>("failures") + 1;
            storage.IndexingStats.UpdateKey(index);
        }

        public void DecrementIndexingAttempt()
        {
            var index = GetCurrentIndex();
            index["attempts"] = index.Value<int>("attempts") - 1;
            storage.IndexingStats.UpdateKey(index);
       
        }

		public void IncrementReduceSuccessIndexing()
		{
			var index = GetCurrentIndex();
			index["reduce_successes"] = index.Value<int?>("reduce_successes") + 1;
			storage.IndexingStats.UpdateKey(index);
		}

		public void IncrementReduceIndexingFailure()
		{
			var index = GetCurrentIndex();
			index["reduce_failures"] = index.Value<int?>("reduce_failures") + 1;
			storage.IndexingStats.UpdateKey(index);
		}

		public void DecrementReduceIndexingAttempt()
		{
			var index = GetCurrentIndex();
			index["reduce_attempts"] = index.Value<int?>("reduce_attempts") - 1;
			storage.IndexingStats.UpdateKey(index);

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
                throw new ArgumentException("There is already an index with the name: " + name);

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
				throw new ArgumentException("There is no index with the name: " + currentIndex.Value);
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
                throw new ArgumentException("There is no index with the name: " + index);

            var ravenJObject = (RavenJObject)readResult.Key;
            ravenJObject["lastReducedEtag"] = etag.ToByteArray();
            ravenJObject["lastReducedTimestamp"] = timestamp;

            storage.IndexingStats.UpdateKey(ravenJObject);
        }
    }
}