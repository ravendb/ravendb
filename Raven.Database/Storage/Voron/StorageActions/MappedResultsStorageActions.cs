namespace Raven.Database.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;

	using Raven.Abstractions;
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Extensions;
	using Raven.Abstractions.MEF;
	using Raven.Database.Impl;
	using Raven.Database.Indexing;
	using Raven.Database.Plugins;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public class MappedResultsStorageActions : IMappedResultsStorageAction
	{
		private readonly TableStorage tableStorage;

		private readonly IUuidGenerator generator;

		private readonly SnapshotReader snapshot;

		private readonly WriteBatch writeBatch;

		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;

		public MappedResultsStorageActions(TableStorage tableStorage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, SnapshotReader snapshot, WriteBatch writeBatch)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
			this.snapshot = snapshot;
			this.writeBatch = writeBatch;
		}

		public IEnumerable<ReduceKeyAndCount> GetKeysStats(string view, int start, int pageSize)
		{
			var reduceKeysByView = tableStorage.ReduceKeys.GetIndex(Tables.ReduceKeys.Indices.ByView);
			using (var iterator = reduceKeysByView.MultiRead(snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					yield break;

				var count = 0;
				do
				{
					count++;

					if (count < start)
						continue;

					if (count - start - pageSize > 0)
						yield break;

					using (var read = tableStorage.ReduceKeys.Read(snapshot, iterator.CurrentKey))
					{
						var value = read.Stream.ToJObject();

						yield return new ReduceKeyAndCount
									 {
										 Count = value.Value<int>("mappedItemsCount"),
										 Key = value.Value<string>("reduceKey")
									 };
					}
				}
				while (iterator.MoveNext());
			}
		}

		public void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data)
		{
			var ms = new MemoryStream();
			using (var stream = documentCodecs.Aggregate((Stream)ms, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
				data.WriteTo(stream);

			var id = generator.CreateSequentialUuid(UuidType.MappedResults);

			tableStorage.MappedResults.Add(
				writeBatch,
				id.ToString(),
				new RavenJObject
				{
					{ "view", view },
					{ "reduceKey", reduceKey },
					{ "docId", docId },
					{ "etag", id.ToByteArray() },
					{ "bucket", IndexingUtil.MapBucket(docId) },
					{ "timestamp", SystemTime.UtcNow },
					{ "data", ms.ToArray() }
				});
		}

		public void IncrementReduceKeyCounter(string view, string reduceKey, int val)
		{
			throw new NotImplementedException();
		}

		public void DeleteMappedResultsForDocumentId(string documentId, string view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			throw new NotImplementedException();
		}

		public void UpdateRemovedMapReduceStats(string view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			throw new NotImplementedException();
		}

		public void DeleteMappedResultsForView(string view)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<string> GetKeysForIndexForDebug(string indexName, int start, int take)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(string indexName, string key, int start, int take)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string indexName, string key, int level, int start, int take)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(string indexName, int start, int take)
		{
			throw new NotImplementedException();
		}

		public void ScheduleReductions(string view, int level, ReduceKeyAndBucket reduceKeysAndBuckets)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

			var id = generator.CreateSequentialUuid(UuidType.ScheduledReductions);
			var idAsString = id.ToString();

			tableStorage.ScheduledReductions.Add(writeBatch, idAsString, new RavenJObject
			{
				{"view", view},
				{"reduceKey", reduceKeysAndBuckets.ReduceKey},
				{"bucket", reduceKeysAndBuckets.Bucket},
				{"level", level},
				{"etag", id.ToByteArray()},
				{"timestamp", SystemTime.UtcNow}
			});

			scheduledReductionsByView.MultiAdd(writeBatch, view, idAsString);
			scheduledReductionsByViewAndLevelAndReduceKey.MultiAdd(writeBatch, view + "/" + level + "/" + reduceKeysAndBuckets.ReduceKey, idAsString);
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams)
		{
			throw new NotImplementedException();
		}

		public ScheduledReductionInfo DeleteScheduledReduction(List<object> itemsToDelete)
		{
			var result = new ScheduledReductionInfo();
			var hasResult = false;
			var currentEtag = Etag.Empty;
			foreach (RavenJToken token in itemsToDelete)
			{
				var etag = Etag.Parse(token.Value<byte[]>("etag"));
				var etagAsString = etag.ToString();
				using (var read = tableStorage.ScheduledReductions.Read(snapshot, etagAsString))
				{
					if (read == null)
						continue;

					var value = read.Stream.ToJObject();

					if (etag.CompareTo(currentEtag) > 0)
					{
						hasResult = true;
						result.Etag = etag;
						result.Timestamp = value.Value<DateTime>("timestamp");
					}

					var view = value.Value<string>("view");
					var level = value.Value<int>("level");
					var reduceKey = value.Value<string>("reduceKey");

					DeleteScheduledReduction(etagAsString, view, level, reduceKey);
				}
			}

			return hasResult ? result : null;
		}

		public void DeleteScheduledReduction(string view, int level, string reduceKey)
		{
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(snapshot, view + "/" + level + "/" + reduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey;
					DeleteScheduledReduction(id, view, level, reduceKey);
				}
				while (iterator.MoveNext());
			}
		}

		public void PutReducedResult(string name, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			throw new NotImplementedException();
		}

		public void RemoveReduceResults(string indexName, int level, string reduceKey, int sourceBucket)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(string indexName, int take, int limitOfItemsToReduceInSingleStep)
		{
			throw new NotImplementedException();
		}

		public void UpdatePerformedReduceType(string indexName, string reduceKey, ReduceType performedReduceType)
		{
			throw new NotImplementedException();
		}

		public ReduceType GetLastPerformedReduceType(string indexName, string reduceKey)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<int> GetMappedBuckets(string indexName, string reduceKey)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<MappedResultInfo> GetMappedResults(string indexName, IEnumerable<string> keysToReduce, bool loadData)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(string view, int start, int take)
		{
			throw new NotImplementedException();
		}

		private void DeleteScheduledReduction(Slice id, string view, int level, string reduceKey)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

			tableStorage.ScheduledReductions.Delete(writeBatch, id);
			scheduledReductionsByView.MultiDelete(writeBatch, view, id);
			scheduledReductionsByViewAndLevelAndReduceKey.MultiDelete(writeBatch, view + "/" + level + "/" + reduceKey, id);
		}
	}
}