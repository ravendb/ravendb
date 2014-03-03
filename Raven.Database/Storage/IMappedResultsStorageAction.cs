//-----------------------------------------------------------------------
// <copyright file="IMappedResultsStorageAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Database.Indexing;
using Raven.Abstractions.Data;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IMappedResultsStorageAction
	{
		IEnumerable<ReduceKeyAndCount> GetKeysStats(int view, int start, int pageSize);

		void PutMappedResult(int indexId, string docId, string reduceKey, RavenJObject data);
		void IncrementReduceKeyCounter(int indexId, string reduceKey, int val);
		void DeleteMappedResultsForDocumentId(string documentId, int view, Dictionary<ReduceKeyAndBucket, int> removed);
		void UpdateRemovedMapReduceStats(int indexId, Dictionary<ReduceKeyAndBucket, int> removed);
		void DeleteMappedResultsForView(int indexId);

		IEnumerable<string> GetKeysForIndexForDebug(int index, int start, int take);

		IEnumerable<MappedResultInfo> GetMappedResultsForDebug(int index, string key, int start, int take);
		IEnumerable<MappedResultInfo> GetReducedResultsForDebug(int index, string reduceKey, int level, int start, int take);
		IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(int index, int start, int take);

		void ScheduleReductions(int index, int level, ReduceKeyAndBucket reduceKeysAndBuckets);
		IEnumerable<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams);
		ScheduledReductionInfo DeleteScheduledReduction(IEnumerable<object> itemsToDelete);
		void DeleteScheduledReduction(int index, int level, string reduceKey);
		void PutReducedResult(int index, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data);
		void RemoveReduceResults(int index, int level, string reduceKey, int sourceBucket);
		IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(int index, int take, int limitOfItemsToReduceInSingleStep);
		void UpdatePerformedReduceType(int index, string reduceKey, ReduceType performedReduceType);
		ReduceType GetLastPerformedReduceType(int index, string reduceKey);
		IEnumerable<int> GetMappedBuckets(int index, string reduceKey);
		IEnumerable<MappedResultInfo> GetMappedResults(int index, IEnumerable<string> keysToReduce, bool loadData);
		IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(int view, int start, int take);
	}

	public class GetItemsToReduceParams
	{

		public GetItemsToReduceParams(int index, IEnumerable<string> reduceKeys, int level, bool loadData, ConcurrentSet<object> itemsToDelete)
		{
			Index = index;
			Level = level;
			LoadData = loadData;
			ItemsToDelete = itemsToDelete;
			ItemsAlreadySeen = new HashSet<Tuple<string, int>>();
			ReduceKeys = new HashSet<string>(reduceKeys);
		}

		public int Index { get; private set; }
		public int Level { get; private set; }
		public bool LoadData { get; private set; }
		public int Take { get; set; }
		public ConcurrentSet<object> ItemsToDelete { get; private set; }
		public HashSet<Tuple<string, int>> ItemsAlreadySeen { get; private set; }
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
			if (obj.GetType() != GetType()) return false;
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
		public string ReduceKey { get; set; }
		public DateTime Timestamp { get; set; }
		public Etag Etag { get; set; }

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
