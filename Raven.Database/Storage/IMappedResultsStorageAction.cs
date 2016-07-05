//-----------------------------------------------------------------------
// <copyright file="IMappedResultsStorageAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Sparrow.Collections;
using Sparrow;

namespace Raven.Database.Storage
{
    public interface IMappedResultsStorageAction
    {
        IEnumerable<ReduceKeyAndCount> GetKeysStats(int view, int start, int pageSize);

        void PutMappedResult(int indexId, string docId, string reduceKey, RavenJObject data);
        void IncrementReduceKeyCounter(int indexId, string reduceKey, int val);
        void DeleteMappedResultsForDocumentId(string documentId, int view, Dictionary<ReduceKeyAndBucket, int> removed);
        void UpdateRemovedMapReduceStats(int indexId, Dictionary<ReduceKeyAndBucket, int> removed, CancellationToken token);
        void DeleteMappedResultsForView(int indexId, CancellationToken token);

        IEnumerable<string> GetKeysForIndexForDebug(int index, string startsWith, string sourceId, int start, int take);
        IEnumerable<string> GetSourcesForIndexForDebug(int index, string startsWith, int take);

        IEnumerable<MappedResultInfo> GetMappedResultsForDebug(int index, string key, int start, int take);
        IEnumerable<MappedResultInfo> GetReducedResultsForDebug(int index, string reduceKey, int level, int start, int take);
        IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(int index, int start, int take);

        void ScheduleReductions(int index, int level, ReduceKeyAndBucket reduceKeysAndBuckets);
        IList<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams, CancellationToken cancellationToken);
        ScheduledReductionInfo DeleteScheduledReduction(IEnumerable<object> itemsToDelete, CancellationToken token);
        Dictionary<int, RemainingReductionPerLevel> GetRemainingScheduledReductionPerIndex();
        void DeleteScheduledReduction(int index, int level, string reduceKey);
        void PutReducedResult(int index, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data);
        void RemoveReduceResults(int index, int level, string reduceKey, int sourceBucket);
        IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(int index, int take, int limitOfItemsToReduceInSingleStep, CancellationToken cancellationToken);
        void UpdatePerformedReduceType(int index, string reduceKey, ReduceType performedReduceType, bool skipAdd = false);
        ReduceType GetLastPerformedReduceType(int index, string reduceKey);
        IEnumerable<int> GetMappedBuckets(int index, string reduceKey, CancellationToken cancellationToken);

        List<MappedResultInfo> GetMappedResults(int view, HashSet<string> keysLeftToReduce, bool loadData, int take, HashSet<string> keysReturned, CancellationToken cancellationToken, List<MappedResultInfo> outputCollection = null);
    
        IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(int view, int start, int take);
        Dictionary<int, long> DeleteObsoleteScheduledReductions(List<int> mapReduceIndexIds, long delete);
    }

    public class GetItemsToReduceParams
    {

        public GetItemsToReduceParams(int index, HashSet<string> reduceKeys, int level, bool loadData, ConcurrentSet<object> itemsToDelete)
        {
            Index = index;
            Level = level;
            LoadData = loadData;
            ItemsToDelete = itemsToDelete;
            ReduceKeys = reduceKeys;
        }

        public int Index { get; private set; }
        public int Level { get; private set; }
        public bool LoadData { get; private set; }
        public int Take { get; set; }
        public ConcurrentSet<object> ItemsToDelete { get; private set; }
        public ReduceKeyAndBucket LastReduceKeyAndBucket { get; set; }
        public HashSet<string> ReduceKeys { get; private set; }
    }

    public class ReduceKeyAndBucket
    {
        public readonly int Bucket;
        public readonly string ReduceKey;

        public ReduceKeyAndBucket(int bucket, string reduceKey)
        {
            if (reduceKey == null) throw new ArgumentNullException("reduceKey");
            Bucket = bucket;
            ReduceKey = reduceKey;
        }

        public override string ToString()
        {
            return Bucket + " - " + ReduceKey;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;

            var y = (ReduceKeyAndBucket) obj;

            return this.Bucket == y.Bucket && string.Equals(this.ReduceKey, y.ReduceKey);
        }

        public override int GetHashCode()
        {
            unsafe
            {
                fixed (char* buffer = this.ReduceKey)
                {
                    return this.Bucket ^ (int)Hashing.XXHash32.CalculateInline((byte*)buffer, this.ReduceKey.Length * sizeof(char));
                }
            }
        }
    }

    internal sealed class ReduceKeyAndBucketEqualityComparer : IEqualityComparer<ReduceKeyAndBucket>
    {
        public static ReduceKeyAndBucketEqualityComparer Instance = new ReduceKeyAndBucketEqualityComparer();

        public bool Equals(ReduceKeyAndBucket x, ReduceKeyAndBucket y)
        {
            return x.Bucket == y.Bucket && string.Equals(x.ReduceKey, y.ReduceKey);
        }

        public int GetHashCode(ReduceKeyAndBucket obj)
        {
            unsafe
            {
                fixed (char* buffer = obj.ReduceKey)
                {
                    return obj.Bucket ^ (int)Hashing.XXHash32.CalculateInline((byte*)buffer, obj.ReduceKey.Length * sizeof(char));
                }
            }
        }
    }

    public class ScheduledReductionInfo
    {
        public DateTime Timestamp { get; set; }
        public Etag Etag { get; set; }
    }

    public class ScheduledReductionDebugInfo
    {
        public DateTime Timestamp { get; set; }
        public Guid Etag { get; set; }
        public string Key { get; set; }
        public int Level { get; set; }
        public int Bucket { get; set; }
    }

    public class MappedResultInfo
    {
        // These 3 are a compound key. 
        public string ReduceKey { get; set; }        
        public int Bucket { get; set; }


        public DateTime Timestamp { get; set; }
        public Etag Etag { get; set; }

        public RavenJObject Data { get; set; }

        public string Source { get; set; }

        [JsonIgnore]
        public int Size { get; set; }

        public override string ToString()
        {
            return string.Format("{0},{1}: {2}", ReduceKey, Bucket, Data == null ? "null" : Data.ToString(Formatting.None));
        }
    }

    public class ReduceTypePerKey
    {
        public ReduceTypePerKey(string reduceKey, ReduceType type)
        {
            ReduceKey = reduceKey;
            OperationTypeToPerform = type;
        }

        public string ReduceKey { get; set; }
        public ReduceType OperationTypeToPerform { get; set; }
    }

    public class ReduceKeyAndCount
    {
        public int Count { get; set; }
        public string Key { get; set; }
    }
}
