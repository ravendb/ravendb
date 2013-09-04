namespace Raven.Database.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.Globalization;
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

	using Index = Raven.Database.Storage.Voron.Impl.Index;

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
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					using (var read = tableStorage.ReduceKeys.Read(snapshot, iterator.CurrentKey))
					{
						var value = read.Stream.ToJObject();

						yield return new ReduceKeyAndCount
									 {
										 Count = value.Value<int>("mappedItemsCount"),
										 Key = value.Value<string>("reduceKey")
									 };

						count++;
					}
				}
				while (iterator.MoveNext() && count < pageSize);
			}
		}

		public void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data)
		{
			var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);

			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			var ms = new MemoryStream();
			using (var stream = documentCodecs.Aggregate((Stream)ms, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
				data.WriteTo(stream);

			var id = generator.CreateSequentialUuid(UuidType.MappedResults);
			var idAsString = id.ToString();
			var bucket = IndexingUtil.MapBucket(docId);

			tableStorage.MappedResults.Add(
				writeBatch,
				idAsString,
				new RavenJObject
				{
					{ "view", view },
					{ "reduceKey", reduceKey },
					{ "docId", docId },
					{ "etag", id.ToByteArray() },
					{ "bucket", bucket },
					{ "timestamp", SystemTime.UtcNow }
				}, 0);

			mappedResultsData.Add(writeBatch, idAsString, ms, 0);

			mappedResultsByViewAndDocumentId.MultiAdd(writeBatch, view + "/" + docId, idAsString);
			mappedResultsByView.MultiAdd(writeBatch, view, idAsString);
			mappedResultsByViewAndReduceKey.MultiAdd(writeBatch, view + "/" + reduceKey, idAsString);
			mappedResultsByViewAndReduceKeyAndSourceBucket.MultiAdd(writeBatch, view + "/" + reduceKey + "/" + bucket, idAsString);
		}

		public void IncrementReduceKeyCounter(string view, string reduceKey, int val)
		{
			var key = view + "/" + reduceKey;

			ushort version;
			var value = Load(tableStorage.ReduceKeys, key, out version);

			if (value == null)
			{
				if (val <= 0)
					return;

				AddReduceKey(key, view, reduceKey, ReduceType.None, val, 0);
				return;
			}

			var decrementedValue = value.Value<int>("mappedItemsCount") + val;

			if (decrementedValue > 0)
			{
				AddReduceKey(key, view, reduceKey, ReduceType.None, decrementedValue, version);
			}
			else
			{
				DeleteReduceKey(key, view, version);
			}
		}

		public void DeleteMappedResultsForDocumentId(string documentId, string view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			var viewAndDocumentId = view + "/" + documentId;

			var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			using (var iterator = mappedResultsByViewAndDocumentId.MultiRead(snapshot, viewAndDocumentId))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					ushort version;
					var value = Load(tableStorage.MappedResults, iterator.CurrentKey, out version);
					var reduceKey = value.Value<string>("reduceKey");
					var bucket = value.Value<int>("bucket");

					DeleteMappedResult(iterator.CurrentKey, view, documentId, reduceKey, bucket.ToString(CultureInfo.InvariantCulture));

					var reduceKeyAndBucket = new ReduceKeyAndBucket(bucket, reduceKey);
					removed[reduceKeyAndBucket] = removed.GetOrDefault(reduceKeyAndBucket) + 1;
				}
				while (iterator.MoveNext());
			}
		}

		public void UpdateRemovedMapReduceStats(string view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			var statsByKey = new Dictionary<string, int>();
			foreach (var reduceKeyAndBucket in removed)
			{
				statsByKey[reduceKeyAndBucket.Key.ReduceKey] = statsByKey.GetOrDefault(reduceKeyAndBucket.Key.ReduceKey) - reduceKeyAndBucket.Value;
			}

			foreach (var reduceKeyStat in statsByKey)
			{
				IncrementReduceKeyCounter(view, reduceKeyStat.Key, reduceKeyStat.Value);
			}
		}

		public void DeleteMappedResultsForView(string view)
		{
			var statsByKey = new Dictionary<string, int>();
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);

			using (var iterator = mappedResultsByView.MultiRead(snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					ushort version;
					var value = Load(tableStorage.MappedResults, iterator.CurrentKey, out version);
					var reduceKey = value.Value<string>("reduceKey");
					var bucket = value.Value<string>("bucket");

					DeleteMappedResult(iterator.CurrentKey, view, value.Value<string>("docId"), reduceKey, bucket);

					statsByKey[reduceKey] = statsByKey.GetOrDefault(reduceKey) - 1;
				}
				while (iterator.MoveNext());
			}

			foreach (var reduceKeyStat in statsByKey)
			{
				IncrementReduceKeyCounter(view, reduceKeyStat.Key, reduceKeyStat.Value);
			}
		}

		public IEnumerable<string> GetKeysForIndexForDebug(string view, int start, int take)
		{
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			using (var iterator = mappedResultsByView.MultiRead(snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<string>();

				var results = new List<string>();
				do
				{
					ushort version;
					var value = Load(tableStorage.MappedResults, iterator.CurrentKey, out version);

					results.Add(value.Value<string>("reduceKey"));
				}
				while (iterator.MoveNext());

				return results
					.Distinct()
					.Skip(start)
					.Take(take);
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(string view, string key, int start, int take)
		{
			var viewAndReduceKey = view + "/" + key;
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(snapshot, viewAndReduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = Load(tableStorage.MappedResults, iterator.CurrentKey, out version);
					var size = tableStorage.MappedResults.GetDataSize(snapshot, iterator.CurrentKey);
					yield return ConvertToMappedResultInfo(iterator.CurrentKey, value, size, true, mappedResultsData);

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string view, string key, int level, int start, int take)
		{
			var viewAndReduceKeyAndLevel = view + "/" + key + "/" + level;
			var reduceResultsByViewAndReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			using (var iterator = reduceResultsByViewAndReduceKeyAndLevel.MultiRead(snapshot, viewAndReduceKeyAndLevel))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = Load(tableStorage.ReduceResults, iterator.CurrentKey, out version);
					var size = tableStorage.ReduceResults.GetDataSize(snapshot, iterator.CurrentKey);

					yield return ConvertToMappedResultInfo(iterator.CurrentKey, value, size, true, reduceResultsData);

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(string view, int start, int take)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			using (var iterator = scheduledReductionsByView.MultiRead(snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = Load(tableStorage.ScheduledReductions, iterator.CurrentKey, out version);

					yield return new ScheduledReductionDebugInfo
					{
						Key = value.Value<string>("reduceKey"),
						Bucket = value.Value<int>("bucket"),
						Etag = new Guid(value.Value<byte[]>("etag")),
						Level = value.Value<int>("level"),
						Timestamp = value.Value<DateTime>("timestamp"),
					};

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public void ScheduleReductions(string view, int level, ReduceKeyAndBucket reduceKeysAndBuckets)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			var scheduledReductionsByViewAndLevel = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);

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
			scheduledReductionsByViewAndLevel.MultiAdd(writeBatch, view + "/" + level, idAsString);
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams)
		{
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

			var seenLocally = new HashSet<Tuple<string, int>>();
			foreach (var reduceKey in getItemsToReduceParams.ReduceKeys.ToArray())
			{
				var viewAndLevelAndReduceKey = getItemsToReduceParams.Index + "/" + getItemsToReduceParams.Level + "/" + reduceKey;
				using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(snapshot, viewAndLevelAndReduceKey))
				{
					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						ushort version;
						var value = Load(tableStorage.ScheduledReductions, iterator.CurrentKey, out version);

						var indexFromDb = value.Value<string>("view");
						var levelFromDb = value.Value<int>("level");
						var reduceKeyFromDb = value.Value<string>("reduceKey");

						if (string.Equals(indexFromDb, getItemsToReduceParams.Index, StringComparison.OrdinalIgnoreCase) == false ||
						levelFromDb != getItemsToReduceParams.Level)
							break;

						if (string.Equals(reduceKeyFromDb, reduceKey, StringComparison.Ordinal) == false)
							break;

						var bucket = value.Value<int>("bucket");
						var rowKey = Tuple.Create(reduceKeyFromDb, bucket);
						var thisIsNewScheduledReductionRow = getItemsToReduceParams.ItemsToDelete.Contains(value, RavenJTokenEqualityComparer.Default) == false;
						var neverSeenThisKeyAndBucket = getItemsToReduceParams.ItemsAlreadySeen.Add(rowKey);
						if (thisIsNewScheduledReductionRow || neverSeenThisKeyAndBucket)
						{
							if (seenLocally.Add(rowKey))
							{
								foreach (var mappedResultInfo in GetResultsForBucket(getItemsToReduceParams.Index, getItemsToReduceParams.Level, reduceKeyFromDb, bucket, getItemsToReduceParams.LoadData))
								{
									getItemsToReduceParams.Take--;
									yield return mappedResultInfo;
								}
							}
						}

						if (thisIsNewScheduledReductionRow)
							getItemsToReduceParams.ItemsToDelete.Add(value);

						if (getItemsToReduceParams.Take <= 0)
							yield break;
					}
					while (iterator.MoveNext());
				}

				getItemsToReduceParams.ReduceKeys.Remove(reduceKey);

				if (getItemsToReduceParams.Take <= 0)
					break;
			}
		}

		private IEnumerable<MappedResultInfo> GetResultsForBucket(string view, int level, string reduceKey, int bucket, bool loadData)
		{
			switch (level)
			{
				case 0:
					return GetMappedResultsForBucket(view, reduceKey, bucket, loadData);
				case 1:
				case 2:
					return GetReducedResultsForBucket(view, reduceKey, level, bucket, loadData);
				default:
					throw new ArgumentException("Invalid level: " + level);
			}
		}

		private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(string view, string reduceKey, int level, int bucket, bool loadData)
		{
			var viewAndReduceKeyAndLevelAndSourceBucket = view + "/" + reduceKey + "/" + level + "/" + bucket;

			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);
			using (var iterator = reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiRead(snapshot, viewAndReduceKeyAndLevelAndSourceBucket))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
				{
					yield return new MappedResultInfo
								 {
									 Bucket = bucket,
									 ReduceKey = reduceKey
								 };

					yield break;
				}

				do
				{
					ushort version;
					var value = Load(tableStorage.ReduceResults, iterator.CurrentKey, out version);
					var size = tableStorage.ReduceResults.GetDataSize(snapshot, iterator.CurrentKey);

					yield return ConvertToMappedResultInfo(iterator.CurrentKey, value, size, loadData, reduceResultsData);
				}
				while (iterator.MoveNext());
			}
		}

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(string view, string reduceKey, int bucket, bool loadData)
		{
			var viewAndReduceKeyAndSourceBucket = view + "/" + reduceKey + "/" + bucket;

			var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndReduceKeyAndSourceBucket.MultiRead(snapshot, viewAndReduceKeyAndSourceBucket))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
				{
					yield return new MappedResultInfo
					{
						Bucket = bucket,
						ReduceKey = reduceKey
					};

					yield break;
				}

				do
				{
					ushort version;
					var value = Load(tableStorage.MappedResults, iterator.CurrentKey, out version);
					var size = tableStorage.MappedResults.GetDataSize(snapshot, iterator.CurrentKey);

					yield return ConvertToMappedResultInfo(iterator.CurrentKey, value, size, loadData, mappedResultsData);
				}
				while (iterator.MoveNext());
			}
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

		public void PutReducedResult(string view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
			var reduceResultsByViewAndReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			var ms = new MemoryStream();
			using (var stream = documentCodecs.Aggregate((Stream)ms, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
				data.WriteTo(stream);

			var id = generator.CreateSequentialUuid(UuidType.MappedResults);
			var idAsString = id.ToString();

			tableStorage.ReduceResults.Add(
				writeBatch,
				idAsString,
				new RavenJObject
				{
					{ "view", view },
					{ "etag", id.ToByteArray() },
					{ "reduceKey", reduceKey },
					{ "level", level },
					{ "sourceBucket", sourceBucket },
					{ "bucket", bucket },
					{ "timestamp", SystemTime.UtcNow }
				},
				0);

			reduceResultsData.Add(writeBatch, idAsString, ms, 0);

			var viewAndReduceKeyAndLevelAndSourceBucket = view + "/" + reduceKey + "/" + level + "/" + sourceBucket;
			var viewAndReduceKeyAndLevel = view + "/" + reduceKey + "/" + level;

			reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiAdd(writeBatch, viewAndReduceKeyAndLevelAndSourceBucket, idAsString);
			reduceResultsByViewAndReduceKeyAndLevel.MultiAdd(writeBatch, viewAndReduceKeyAndLevel, idAsString);
		}

		public void RemoveReduceResults(string view, int level, string reduceKey, int sourceBucket)
		{
			var viewAndReduceKeyAndLevelAndSourceBucket = view + "/" + reduceKey + "/" + level + "/" + sourceBucket;
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);

			using (var iterator = reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiRead(snapshot, viewAndReduceKeyAndLevelAndSourceBucket))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					RemoveReduceResult(iterator.CurrentKey, view, level, reduceKey, sourceBucket);
				}
				while (iterator.MoveNext());
			}
		}

		public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(string view, int take, int limitOfItemsToReduceInSingleStep)
		{
			var allKeysToReduce = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

			var viewAndLevel = view + "/" + 0;
			var scheduledReductionsByViewAndLevel = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);
			using (var iterator = scheduledReductionsByViewAndLevel.MultiRead(snapshot, viewAndLevel))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<ReduceTypePerKey>();

				do
				{
					ushort version;
					var value = this.Load(tableStorage.ScheduledReductions, iterator.CurrentKey, out version);

					allKeysToReduce.Add(value.Value<string>("reduceKey"));
				}
				while (iterator.MoveNext());
			}

			var reduceTypesPerKeys = allKeysToReduce.ToDictionary(x => x, x => ReduceType.SingleStep);

			foreach (var reduceKey in allKeysToReduce)
			{
				var count = GetNumberOfMappedItemsPerReduceKey(view, reduceKey);
				if (count >= limitOfItemsToReduceInSingleStep)
				{
					reduceTypesPerKeys[reduceKey] = ReduceType.MultiStep;
				}
			}

			return reduceTypesPerKeys.Select(x => new ReduceTypePerKey(x.Key, x.Value));
		}

		private int GetNumberOfMappedItemsPerReduceKey(string view, string reduceKey)
		{
			var key = view + "/" + reduceKey;

			ushort version;
			var value = Load(tableStorage.ReduceKeys, key, out version);
			if (value == null)
				return 0;

			return value.Value<int>("mappedItemsCount");
		}

		public void UpdatePerformedReduceType(string view, string reduceKey, ReduceType reduceType)
		{
			var key = view + "/" + reduceKey;

			ushort version;
			var value = Load(tableStorage.ReduceKeys, key, out version);
			if (value == null)
			{
				AddReduceKey(key, view, reduceKey, reduceType, 0, 0);
				return;
			}

			value["reduceType"] = (int)reduceType;
			tableStorage.ReduceKeys.Add(writeBatch, key, value, version);
		}

		private void AddReduceKey(string key, string view, string reduceKey, ReduceType reduceType, int mappedItemsCount, ushort? expectedVersion)
		{
			var reduceKeysByView = tableStorage.ReduceKeys.GetIndex(Tables.ReduceKeys.Indices.ByView);

			tableStorage.ReduceKeys.Add(
						writeBatch,
						key,
						new RavenJObject
						{
							{ "view", view },
							{ "reduceKey", reduceKey },
							{ "reduceType", (int)reduceType },
							{ "mappedItemsCount", mappedItemsCount }
						}, expectedVersion);

			reduceKeysByView.MultiAdd(writeBatch, view, key);
		}

		private void DeleteReduceKey(string key, string view, ushort? expectedVersion)
		{
			var reduceKeysByView = tableStorage.ReduceKeys.GetIndex(Tables.ReduceKeys.Indices.ByView);

			tableStorage.ReduceKeys.Delete(writeBatch, key, expectedVersion);
			reduceKeysByView.MultiDelete(writeBatch, view, key);
		}

		public ReduceType GetLastPerformedReduceType(string view, string reduceKey)
		{
			var key = view + "/" + reduceKey;

			ushort version;
			var value = Load(tableStorage.ReduceKeys, key, out version);
			if (value == null)
				return ReduceType.None;

			return (ReduceType)value.Value<int>("reduceType");
		}

		public IEnumerable<int> GetMappedBuckets(string view, string reduceKey)
		{
			var viewAndReduceKey = view + "/" + reduceKey;
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);

			using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(snapshot, viewAndReduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<int>();

				var results = new List<int>();
				do
				{
					ushort version;
					var value = Load(tableStorage.MappedResults, iterator.CurrentKey, out version);

					results.Add(value.Value<int>("bucket"));
				}
				while (iterator.MoveNext());

				return results.Distinct();
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResults(string view, IEnumerable<string> keysToReduce, bool loadData)
		{
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			foreach (var reduceKey in keysToReduce)
			{
				var viewAndReduceKey = view + "/" + reduceKey;
				using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(snapshot, viewAndReduceKey))
				{
					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						ushort version;
						var value = Load(tableStorage.MappedResults, iterator.CurrentKey, out version);
						var size = tableStorage.MappedResults.GetDataSize(snapshot, iterator.CurrentKey);

						yield return ConvertToMappedResultInfo(iterator.CurrentKey, value, size, loadData, mappedResultsData);
					}
					while (iterator.MoveNext());
				}
			}
		}

		private MappedResultInfo ConvertToMappedResultInfo(Slice key, RavenJObject value, int size, bool loadData, Index dataIndex)
		{
			return new MappedResultInfo
			{
				ReduceKey = value.Value<string>("reduceKey"),
				Etag = Etag.Parse(value.Value<byte[]>("etag")),
				Timestamp = value.Value<DateTime>("timestamp"),
				Bucket = value.Value<int>("bucket"),
				Source = value.Value<string>("docId"),
				Size = size,
				Data = loadData ? LoadMappedResult(key, value, dataIndex) : null
			};
		}

		private RavenJObject LoadMappedResult(Slice key, RavenJObject value, Index dataIndex)
		{
			var reduceKey = value.Value<string>("reduceKey");

			using (var read = dataIndex.Read(snapshot, key))
			{
				if (read == null)
					return null;

				using (var stream = documentCodecs.Aggregate(read.Stream, (ds, codec) => codec.Decode(reduceKey, null, ds)))
					return stream.ToJObject();
			}
		}

		public IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(string view, int start, int take)
		{
			var reduceKeysByView = tableStorage.ReduceKeys.GetIndex(Tables.ReduceKeys.Indices.ByView);
			using (var iterator = reduceKeysByView.MultiRead(snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = Load(tableStorage.ReduceKeys, iterator.CurrentKey, out version);

					yield return new ReduceTypePerKey(value.Value<string>("reduceKey"), (ReduceType)value.Value<int>("reduceType"));

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		private void DeleteScheduledReduction(Slice id, string view, int level, string reduceKey)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			var scheduledReductionsByViewAndLevel = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);

			tableStorage.ScheduledReductions.Delete(writeBatch, id);
			scheduledReductionsByView.MultiDelete(writeBatch, view, id);
			scheduledReductionsByViewAndLevelAndReduceKey.MultiDelete(writeBatch, view + "/" + level + "/" + reduceKey, id);
			scheduledReductionsByViewAndLevel.MultiDelete(writeBatch, view + "/" + level, id);
		}

		private void DeleteMappedResult(Slice id, string view, string documentId, string reduceKey, string bucket)
		{
			var viewAndDocumentId = view + "/" + documentId;
			var viewAndReduceKey = view + "/" + reduceKey;
			var viewAndReduceKeyAndSourceBucket = view + "/" + reduceKey + "/" + bucket;
			var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			tableStorage.MappedResults.Delete(writeBatch, id);
			mappedResultsByViewAndDocumentId.MultiDelete(writeBatch, viewAndDocumentId, id);
			mappedResultsByView.MultiDelete(writeBatch, view, id);
			mappedResultsByViewAndReduceKey.MultiDelete(writeBatch, viewAndReduceKey, id);
			mappedResultsByViewAndReduceKeyAndSourceBucket.MultiDelete(writeBatch, viewAndReduceKeyAndSourceBucket, id);
			mappedResultsData.Delete(writeBatch, id);
		}

		private void RemoveReduceResult(Slice id, string view, int level, string reduceKey, int sourceBucket)
		{
			var viewAndReduceKeyAndLevelAndSourceBucket = view + "/" + reduceKey + "/" + level + "/" + sourceBucket;
			var viewAndReduceKeyAndLevel = view + "/" + reduceKey + "/" + level;
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
			var reduceResultsByViewAndReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			tableStorage.ReduceResults.Delete(writeBatch, id);
			reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiDelete(writeBatch, viewAndReduceKeyAndLevelAndSourceBucket, id);
			reduceResultsByViewAndReduceKeyAndLevel.MultiDelete(writeBatch, viewAndReduceKeyAndLevel, id);
			reduceResultsData.Delete(writeBatch, id);
		}

		private RavenJObject Load(Table table, Slice key, out ushort version)
		{
			using (var read = table.Read(snapshot, key))
			{
				if (read == null)
				{
					version = 0;
					return null;
				}

				version = read.Version;
				return read.Stream.ToJObject();
			}
		}
	}
}