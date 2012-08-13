//-----------------------------------------------------------------------
// <copyright file="IMappedResultsStorageAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IMappedResultsStorageAction
	{
		void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data);
		IEnumerable<RavenJObject> GetMappedResults(params GetMappedResultsParams[] getMappedResultsParams);
		void DeleteMappedResultsForDocumentId(string documentId, string view, HashSet<ReduceKeyAndBucket> removed);
		void DeleteMappedResultsForView(string view);
		IEnumerable<MappedResultInfo> GetMappedResultsReduceKeysAfter(string indexName, Guid lastReducedEtag, bool loadData, int take);
		void ScheduleReductions(string view, IEnumerable<ReduceKeyAndBucket> reduceKeysAndBukcets);
		IEnumerable<MappedResultInfo> GetItemsToReduce(string index, int level, int take);
		void PutReducedResult(string name, string reduceKey, int level, int bucket, RavenJObject data);
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

	public class MappedResultInfo
	{
		public string ReduceKey { get; set; }
		public DateTime Timestamp { get; set; }
		public Guid Etag { get; set; }

		public RavenJObject Data { get; set; }
		public int Size { get; set; }
		public int Bucket { get; set; }
	}
}
