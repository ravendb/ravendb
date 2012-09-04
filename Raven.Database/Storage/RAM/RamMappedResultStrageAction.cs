using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Storage.RAM
{
	class RamMappedResultStrageAction : IMappedResultsStorageAction
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamMappedResultStrageAction(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		public void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data)
		{
			var etag = generator.CreateSequentialUuid();
			var mapBucket = IndexingUtil.MapBucket(docId);

			var mappedResultToAdd = new MappedResultsWrapper
			{
				DocumentKey = docId,
				View = view,
				MappedResultInfo = new MappedResultInfo
				{
					Bucket = mapBucket,
					Data = data,
					ReduceKey = reduceKey,
					Timestamp = SystemTime.UtcNow,
					Etag = etag
				}
			};

			var mappedResult = state.MappedResults.GetOrAdd(view);

			mappedResult.Add(mappedResultToAdd);
		}

		public void DeleteMappedResultsForDocumentId(string documentId, string view, HashSet<ReduceKeyAndBucket> removed)
		{
			var mappedResult = state.MappedResults.GetOrDefault(view);
			if (mappedResult == null)
				return;

			var docsToDelete = mappedResult.Where(wrapper => wrapper.DocumentKey == documentId).ToList();
			if (docsToDelete.Count == 0)
				return;

			foreach (var mappedResultsWrapper in docsToDelete)
			{

				var reduceKey = mappedResultsWrapper.MappedResultInfo.ReduceKey;
				var bucket = mappedResultsWrapper.MappedResultInfo.Bucket;

				removed.Add(new ReduceKeyAndBucket(bucket, reduceKey));
				//Api.JetDelete(session, MappedResults);
			}
		}

		public void DeleteMappedResultsForView(string view)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsReduceKeysAfter(string indexName, Guid lastReducedEtag, bool loadData, int take)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(string indexName, string key, int take)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_reduce_key_and_bucket");
			Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, key, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
				yield break;

			while (take > 0)
			{
				take -= 1;

				var indexNameFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode, RetrieveColumnGrbit.RetrieveFromIndex);
				var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
				if (string.Equals(indexNameFromDb, indexName, StringComparison.InvariantCultureIgnoreCase) == false ||
					string.Equals(key, keyFromDb, StringComparison.InvariantCultureIgnoreCase) == false)
					break;

				var bucket = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
				yield return new MappedResultInfo
				{
					ReduceKey = keyFromDb,
					Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
					Timestamp = Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).Value,
					Data = LoadMappedResults(keyFromDb),
					Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0,
					Bucket = bucket,
					Source = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"], Encoding.Unicode)
				};

				if (Api.TryMoveNext(session, MappedResults) == false)
					break;
			}
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string indexName, string key, int level, int take)
		{
			throw new NotImplementedException();
		}

		public void ScheduleReductions(string view, int level, IEnumerable<ReduceKeyAndBucket> reduceKeysAndBuckets)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(string index, int level, int take, List<object> itemsToDelete)
		{
			throw new NotImplementedException();
		}

		public ScheduledReductionInfo DeleteScheduledReduction(List<object> itemsToDelete)
		{
			throw new NotImplementedException();
		}

		public void PutReducedResult(string name, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			var etag = generator.CreateSequentialUuid();

			using (var update = new Update(session, ReducedResults, JET_prep.Insert))
			{
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["level"], level);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["bucket"], bucket);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["source_bucket"], sourceBucket);

				using (Stream stream = new BufferedStream(new ColumnStream(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"])))
				{
					using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
					{
						data.WriteTo(dataStream);
						dataStream.Flush();
					}
				}

				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["etag"], etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["timestamp"], SystemTime.UtcNow);

				update.Save();
			}
		}

		public void RemoveReduceResults(string indexName, int level, string reduceKey, int sourceBucket)
		{
			throw new NotImplementedException();
		}
	}
}
