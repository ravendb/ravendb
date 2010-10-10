using System;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Storage.StorageActions;
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

        private JObject GetCurrentIndex()
        {
            var readResult = storage.IndexingStats.Read(new JObject { { "index", currentIndex.Value } });
            if (readResult == null)
                throw new ArgumentException("There is no index with the name: " + currentIndex.Value);
            return (JObject)readResult.Key;
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

        public IEnumerable<IndexStats> GetIndexesStats()
        {
            foreach (var key in storage.IndexingStats.Keys)
            {
                var readResult = storage.IndexingStats.Read(key);
                if(readResult == null)
                    continue;
                yield return new IndexStats
                {
                    IndexingAttempts = readResult.Key.Value<int>("attempts"),
                    IndexingErrors = readResult.Key.Value<int>("failures"),
                    IndexingSuccesses = readResult.Key.Value<int>("successes"),
                    Name = readResult.Key.Value<string>("index"),
                    LastIndexedEtag = new Guid(readResult.Key.Value<byte[]>("lastEtag")),
                    LastIndexedTimestamp = readResult.Key.Value<DateTime>("lastTimestamp")
                };
            }
        }

        public void AddIndex(string name)
        {
            var readResult = storage.IndexingStats.Read(new JObject {{"index", name}});
            if(readResult != null)
                throw new ArgumentException("There is already an index with the name: " + name);

            storage.IndexingStats.Put(new JObject
            {
                {"index", name},
                {"attempts", 0},
                {"sucesses", 0},
                {"failures", 0},
                {"lastEtag", Guid.Empty.ToByteArray()},
                {"lastTimestamp", DateTime.MinValue}
            }, null);
        }

        public void DeleteIndex(string name)
        {
            storage.IndexingStats.Remove(new JObject { { "index", name } });
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
                Name = readResult.Key.Value<string>("index"),
            };
            return indexFailureInformation;
        }

        public void UpdateLastIndexed(string index, Guid etag, DateTime timestamp)
        {
            var readResult = storage.IndexingStats.Read(new JObject { { "index", index } });
            if (readResult == null)
                throw new ArgumentException("There is no index with the name: " + index);

            readResult.Key["lastEtag"] = etag.ToByteArray();
            readResult.Key["lastTimestamp"] = timestamp;

            storage.IndexingStats.Put(readResult.Key, null);
        }
    }
}