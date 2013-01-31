//-----------------------------------------------------------------------
// <copyright file="IMappedResultsStorageAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IMappedResultsStorageAction
	{
		IEnumerable<ReduceKeyAndCount> GetKeysStats(string view, int start, int pageSize);
		
		void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data);
		void DeleteMappedResultsForDocumentId(string documentId, string view, HashSet<ReduceKeyAndBucket> removed);
		void UpdateRemovedMapReduceStats(string view, HashSet<ReduceKeyAndBucket> removed);
		void DeleteMappedResultsForView(string view);

		IEnumerable<string> GetKeysForIndexForDebug(string indexName, int start, int take);

		IEnumerable<MappedResultInfo> GetMappedResultsForDebug(string indexName, string key, int start, int take);
		IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string indexName, string key, int level, int start, int take);

		void ScheduleReductions(string view, int level, IEnumerable<ReduceKeyAndBucket> reduceKeysAndBuckets);
		IEnumerable<MappedResultInfo> GetItemsToReduce(string index, string[] reduceKeys, int level, bool loadData, int take, List<object> itemsToDelete);
		ScheduledReductionInfo DeleteScheduledReduction(List<object> itemsToDelete);
		void PutReducedResult(string name, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data);
		void RemoveReduceResults(string indexName, int level, string reduceKey, int sourceBucket);
		IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(string indexName, int take, int limitOfItemsToReduceInSingleStep);
		void UpdatePerformedReduceType(string indexName, string reduceKey, ReduceType performedReduceType);
		ReduceType GetLastPerformedReduceType(string indexName, string reduceKey);
		IEnumerable<int> GetMappedBuckets(string indexName, string reduceKey);
		IEnumerable<MappedResultInfo> GetMappedResults(string indexName, string[] keysToReduce, bool loadData);
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

		protected bool Equals(ReduceKeyAndBucket other)
		{
			return Bucket == other.Bucket && string.Equals(ReduceKey, other.ReduceKey);
		}

		public override string ToString()
		{
			return Bucket + " - " + ReduceKey;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((ReduceKeyAndBucket) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (Bucket*397) ^ (ReduceKey != null ? ReduceKey.GetHashCode() : 0);
			}
		}
	}

	public class ScheduledReductionInfo
	{
		public DateTime Timestamp { get; set; }
		public Guid Etag { get; set; }
	}

	public class MappedResultInfo
	{
		public string ReduceKey { get; set; }
		public DateTime Timestamp { get; set; }
		public Guid Etag { get; set; }

		public RavenJObject Data { get; set; }
		[JsonIgnore]
		public int Size { get; set; }
		public int Bucket { get; set; }
		public string Source { get; set; }

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

	public enum ReduceType
	{
		None = 0,
		SingleStep = 1,
		MultiStep = 2,
	}
}
