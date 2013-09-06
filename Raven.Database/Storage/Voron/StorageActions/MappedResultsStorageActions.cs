namespace Raven.Database.Storage.Voron.StorageActions
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

	public class MappedResultsStorageActions : StorageActionsBase, IMappedResultsStorageAction
	{
		private readonly TableStorage tableStorage;

		private readonly IUuidGenerator generator;

		private readonly WriteBatch writeBatch;

		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;

		public MappedResultsStorageActions(TableStorage tableStorage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, SnapshotReader snapshot, WriteBatch writeBatch)
			: base(snapshot)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
			this.writeBatch = writeBatch;
		}

		public IEnumerable<ReduceKeyAndCount> GetKeysStats(string view, int start, int pageSize)
		{
			var reduceKeyCountsByView = this.tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);
			using (var iterator = reduceKeyCountsByView.MultiRead(this.Snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					using (var read = this.tableStorage.ReduceKeyCounts.Read(this.Snapshot, iterator.CurrentKey))
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
			var mappedResultsByViewAndDocumentId = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			var mappedResultsByView = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			var mappedResultsByViewAndReduceKey = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsByViewAndReduceKeyAndSourceBucket = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);

			var mappedResultsData = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			var ms = new MemoryStream();
			using (var stream = this.documentCodecs.Aggregate((Stream)ms, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
				data.WriteTo(stream);

			var id = this.generator.CreateSequentialUuid(UuidType.MappedResults);
			var idAsString = id.ToString();
			var bucket = IndexingUtil.MapBucket(docId);

			this.tableStorage.MappedResults.Add(
				this.writeBatch,
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

			mappedResultsData.Add(this.writeBatch, idAsString, ms, 0);

			mappedResultsByViewAndDocumentId.MultiAdd(this.writeBatch, this.CreateKey(view, docId), idAsString);
			mappedResultsByView.MultiAdd(this.writeBatch, view, idAsString);
			mappedResultsByViewAndReduceKey.MultiAdd(this.writeBatch, this.CreateKey(view, reduceKey), idAsString);
			mappedResultsByViewAndReduceKeyAndSourceBucket.MultiAdd(this.writeBatch, this.CreateKey(view, reduceKey, bucket), idAsString);
		}

		public void IncrementReduceKeyCounter(string view, string reduceKey, int val)
		{
			var key = this.CreateKey(view, reduceKey);

			ushort version;
			var value = this.LoadJson(this.tableStorage.ReduceKeyCounts, key, out version);

			var newValue = val;
			if (value != null)
				newValue += value.Value<int>("mappedItemsCount");

			this.AddReduceKeyCount(key, view, reduceKey, newValue, version);
		}

		public void DeleteMappedResultsForDocumentId(string documentId, string view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			var viewAndDocumentId = this.CreateKey(view, documentId);

			var mappedResultsByViewAndDocumentId = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			using (var iterator = mappedResultsByViewAndDocumentId.MultiRead(this.Snapshot, viewAndDocumentId))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.MappedResults, iterator.CurrentKey, out version);
					var reduceKey = value.Value<string>("reduceKey");
					var bucket = value.Value<int>("bucket");

					this.DeleteMappedResult(iterator.CurrentKey, view, documentId, reduceKey, bucket.ToString(CultureInfo.InvariantCulture));

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
				this.IncrementReduceKeyCounter(view, reduceKeyStat.Key, reduceKeyStat.Value);
			}
		}

		public void DeleteMappedResultsForView(string view)
		{
			var statsByKey = new Dictionary<string, int>();
			var mappedResultsByView = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);

			using (var iterator = mappedResultsByView.MultiRead(this.Snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.MappedResults, iterator.CurrentKey, out version);
					var reduceKey = value.Value<string>("reduceKey");
					var bucket = value.Value<string>("bucket");

					this.DeleteMappedResult(iterator.CurrentKey, view, value.Value<string>("docId"), reduceKey, bucket);

					statsByKey[reduceKey] = statsByKey.GetOrDefault(reduceKey) - 1;
				}
				while (iterator.MoveNext());
			}

			foreach (var reduceKeyStat in statsByKey)
			{
				this.IncrementReduceKeyCounter(view, reduceKeyStat.Key, reduceKeyStat.Value);
			}
		}

		public IEnumerable<string> GetKeysForIndexForDebug(string view, int start, int take)
		{
			var mappedResultsByView = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			using (var iterator = mappedResultsByView.MultiRead(this.Snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<string>();

				var results = new List<string>();
				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.MappedResults, iterator.CurrentKey, out version);

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
			var viewAndReduceKey = this.CreateKey(view, key);
			var mappedResultsByViewAndReduceKey = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsData = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(this.Snapshot, viewAndReduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.MappedResults, iterator.CurrentKey, out version);
					var size = this.tableStorage.MappedResults.GetDataSize(this.Snapshot, iterator.CurrentKey);
					yield return this.ConvertToMappedResultInfo(iterator.CurrentKey, value, size, true, mappedResultsData);

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string view, string key, int level, int start, int take)
		{
			var viewAndReduceKeyAndLevel = this.CreateKey(view, key, level);
			var reduceResultsByViewAndReduceKeyAndLevel =
				this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
			var reduceResultsData = this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			using (var iterator = reduceResultsByViewAndReduceKeyAndLevel.MultiRead(this.Snapshot, viewAndReduceKeyAndLevel))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.ReduceResults, iterator.CurrentKey, out version);
					var size = this.tableStorage.ReduceResults.GetDataSize(this.Snapshot, iterator.CurrentKey);

					yield return this.ConvertToMappedResultInfo(iterator.CurrentKey, value, size, true, reduceResultsData);

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(string view, int start, int take)
		{
			var scheduledReductionsByView = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			using (var iterator = scheduledReductionsByView.MultiRead(this.Snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.ScheduledReductions, iterator.CurrentKey, out version);

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
			var scheduledReductionsByView = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			var scheduledReductionsByViewAndLevel = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);

			var id = this.generator.CreateSequentialUuid(UuidType.ScheduledReductions);
			var idAsString = id.ToString();

			this.tableStorage.ScheduledReductions.Add(this.writeBatch, idAsString, new RavenJObject
			{
				{"view", view},
				{"reduceKey", reduceKeysAndBuckets.ReduceKey},
				{"bucket", reduceKeysAndBuckets.Bucket},
				{"level", level},
				{"etag", id.ToByteArray()},
				{"timestamp", SystemTime.UtcNow}
			});

			scheduledReductionsByView.MultiAdd(this.writeBatch, view, idAsString);
			scheduledReductionsByViewAndLevelAndReduceKey.MultiAdd(this.writeBatch, this.CreateKey(view, level, reduceKeysAndBuckets.ReduceKey), idAsString);
			scheduledReductionsByViewAndLevel.MultiAdd(this.writeBatch, this.CreateKey(view, level), idAsString);
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams)
		{
			var scheduledReductionsByViewAndLevelAndReduceKey = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

			var seenLocally = new HashSet<Tuple<string, int>>();
			foreach (var reduceKey in getItemsToReduceParams.ReduceKeys.ToArray())
			{
				var viewAndLevelAndReduceKey = this.CreateKey(getItemsToReduceParams.Index, getItemsToReduceParams.Level, reduceKey);
				using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(this.Snapshot, viewAndLevelAndReduceKey))
				{
					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						ushort version;
						var value = this.LoadJson(this.tableStorage.ScheduledReductions, iterator.CurrentKey, out version);

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
								foreach (var mappedResultInfo in this.GetResultsForBucket(getItemsToReduceParams.Index, getItemsToReduceParams.Level, reduceKeyFromDb, bucket, getItemsToReduceParams.LoadData))
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
					return this.GetMappedResultsForBucket(view, reduceKey, bucket, loadData);
				case 1:
				case 2:
					return this.GetReducedResultsForBucket(view, reduceKey, level, bucket, loadData);
				default:
					throw new ArgumentException("Invalid level: " + level);
			}
		}

		private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(string view, string reduceKey, int level, int bucket, bool loadData)
		{
			var viewAndReduceKeyAndLevelAndSourceBucket = this.CreateKey(view, reduceKey, level, bucket);

			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket = this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
			var reduceResultsData = this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);
			using (var iterator = reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiRead(this.Snapshot, viewAndReduceKeyAndLevelAndSourceBucket))
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
					var value = this.LoadJson(this.tableStorage.ReduceResults, iterator.CurrentKey, out version);
					var size = this.tableStorage.ReduceResults.GetDataSize(this.Snapshot, iterator.CurrentKey);

					yield return this.ConvertToMappedResultInfo(iterator.CurrentKey, value, size, loadData, reduceResultsData);
				}
				while (iterator.MoveNext());
			}
		}

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(string view, string reduceKey, int bucket, bool loadData)
		{
			var viewAndReduceKeyAndSourceBucket = this.CreateKey(view, reduceKey, bucket);

			var mappedResultsByViewAndReduceKeyAndSourceBucket = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
			var mappedResultsData = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndReduceKeyAndSourceBucket.MultiRead(this.Snapshot, viewAndReduceKeyAndSourceBucket))
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
					var value = this.LoadJson(this.tableStorage.MappedResults, iterator.CurrentKey, out version);
					var size = this.tableStorage.MappedResults.GetDataSize(this.Snapshot, iterator.CurrentKey);

					yield return this.ConvertToMappedResultInfo(iterator.CurrentKey, value, size, loadData, mappedResultsData);
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
				using (var read = this.tableStorage.ScheduledReductions.Read(this.Snapshot, etagAsString))
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

					this.DeleteScheduledReduction(etagAsString, view, level, reduceKey);
				}
			}

			return hasResult ? result : null;
		}

		public void DeleteScheduledReduction(string view, int level, string reduceKey)
		{
			var scheduledReductionsByViewAndLevelAndReduceKey = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(this.Snapshot, this.CreateKey(view, level, reduceKey)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey;
					this.DeleteScheduledReduction(id, view, level, reduceKey);
				}
				while (iterator.MoveNext());
			}
		}

		public void PutReducedResult(string view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
				this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
			var reduceResultsByViewAndReduceKeyAndLevel =
				this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
			var reduceResultsData = this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			var ms = new MemoryStream();
			using (var stream = this.documentCodecs.Aggregate((Stream)ms, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
				data.WriteTo(stream);

			var id = this.generator.CreateSequentialUuid(UuidType.MappedResults);
			var idAsString = id.ToString();

			this.tableStorage.ReduceResults.Add(
				this.writeBatch,
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

			reduceResultsData.Add(this.writeBatch, idAsString, ms, 0);

			var viewAndReduceKeyAndLevelAndSourceBucket = this.CreateKey(view, reduceKey, level, sourceBucket);
			var viewAndReduceKeyAndLevel = this.CreateKey(view, reduceKey, level);

			reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiAdd(this.writeBatch, viewAndReduceKeyAndLevelAndSourceBucket, idAsString);
			reduceResultsByViewAndReduceKeyAndLevel.MultiAdd(this.writeBatch, viewAndReduceKeyAndLevel, idAsString);
		}

		public void RemoveReduceResults(string view, int level, string reduceKey, int sourceBucket)
		{
			var viewAndReduceKeyAndLevelAndSourceBucket = this.CreateKey(view, reduceKey, level, sourceBucket);
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
				this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);

			using (var iterator = reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiRead(this.Snapshot, viewAndReduceKeyAndLevelAndSourceBucket))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					this.RemoveReduceResult(iterator.CurrentKey, view, level, reduceKey, sourceBucket);
				}
				while (iterator.MoveNext());
			}
		}

		public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(string view, int take, int limitOfItemsToReduceInSingleStep)
		{
			var allKeysToReduce = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

			var viewAndLevel = this.CreateKey(view, 0);
			var scheduledReductionsByViewAndLevel = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);
			using (var iterator = scheduledReductionsByViewAndLevel.MultiRead(this.Snapshot, viewAndLevel))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<ReduceTypePerKey>();

				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.ScheduledReductions, iterator.CurrentKey, out version);

					allKeysToReduce.Add(value.Value<string>("reduceKey"));
				}
				while (iterator.MoveNext());
			}

			var reduceTypesPerKeys = allKeysToReduce.ToDictionary(x => x, x => ReduceType.SingleStep);

			foreach (var reduceKey in allKeysToReduce)
			{
				var count = this.GetNumberOfMappedItemsPerReduceKey(view, reduceKey);
				if (count >= limitOfItemsToReduceInSingleStep)
				{
					reduceTypesPerKeys[reduceKey] = ReduceType.MultiStep;
				}
			}

			return reduceTypesPerKeys.Select(x => new ReduceTypePerKey(x.Key, x.Value));
		}

		private int GetNumberOfMappedItemsPerReduceKey(string view, string reduceKey)
		{
			var key = this.CreateKey(view, reduceKey);

			ushort version;
			var value = this.LoadJson(this.tableStorage.ReduceKeyCounts, key, out version);
			if (value == null)
				return 0;

			return value.Value<int>("mappedItemsCount");
		}

		public void UpdatePerformedReduceType(string view, string reduceKey, ReduceType reduceType)
		{
			var key = this.CreateKey(view, reduceKey);
			var version = this.tableStorage.ReduceKeyTypes.ReadVersion(this.Snapshot, key);

			this.AddReduceKeyType(key, view, reduceKey, reduceType, version);
		}

		private void AddReduceKeyCount(string key, string view, string reduceKey, int count, ushort? expectedVersion)
		{
			var reduceKeyCountsByView = this.tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);

			this.tableStorage.ReduceKeyCounts.Add(
						this.writeBatch,
						key,
						new RavenJObject
						{
							{ "view", view },
							{ "reduceKey", reduceKey },
							{ "mappedItemsCount", count }
						}, expectedVersion);

			reduceKeyCountsByView.MultiAdd(this.writeBatch, view, key);
		}

		private void AddReduceKeyType(string key, string view, string reduceKey, ReduceType status, ushort? expectedVersion)
		{
			var reduceKeyTypesByView = this.tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

			this.tableStorage.ReduceKeyTypes.Add(
						this.writeBatch,
						key,
						new RavenJObject
						{
							{ "view", view },
							{ "reduceKey", reduceKey },
							{ "reduceType", (int)status }
						}, expectedVersion);

			reduceKeyTypesByView.MultiAdd(this.writeBatch, view, key);
		}

		private void DeleteReduceKey(string key, string view, ushort? expectedVersion)
		{
			var reduceKeyCountsByView = this.tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);
			var reduceKeTypesByView = this.tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

			this.tableStorage.ReduceKeyCounts.Delete(this.writeBatch, key, expectedVersion);
			reduceKeyCountsByView.MultiDelete(this.writeBatch, view, key);

			this.tableStorage.ReduceKeyTypes.Delete(this.writeBatch, key, expectedVersion);
			reduceKeTypesByView.MultiDelete(this.writeBatch, view, key);
		}

		public ReduceType GetLastPerformedReduceType(string view, string reduceKey)
		{
			var key = this.CreateKey(view, reduceKey);

			ushort version;
			var value = this.LoadJson(this.tableStorage.ReduceKeyTypes, key, out version);
			if (value == null)
				return ReduceType.None;

			return (ReduceType)value.Value<int>("reduceType");
		}

		public IEnumerable<int> GetMappedBuckets(string view, string reduceKey)
		{
			var viewAndReduceKey = this.CreateKey(view, reduceKey);
			var mappedResultsByViewAndReduceKey = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);

			using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(this.Snapshot, viewAndReduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<int>();

				var results = new List<int>();
				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.MappedResults, iterator.CurrentKey, out version);

					results.Add(value.Value<int>("bucket"));
				}
				while (iterator.MoveNext());

				return results.Distinct();
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResults(string view, IEnumerable<string> keysToReduce, bool loadData)
		{
			var mappedResultsByViewAndReduceKey = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsData = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			foreach (var reduceKey in keysToReduce)
			{
				var viewAndReduceKey = this.CreateKey(view, reduceKey);
				using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(this.Snapshot, viewAndReduceKey))
				{
					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						ushort version;
						var value = this.LoadJson(this.tableStorage.MappedResults, iterator.CurrentKey, out version);
						var size = this.tableStorage.MappedResults.GetDataSize(this.Snapshot, iterator.CurrentKey);

						yield return this.ConvertToMappedResultInfo(iterator.CurrentKey, value, size, loadData, mappedResultsData);
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
				Data = loadData ? this.LoadMappedResult(key, value, dataIndex) : null
			};
		}

		private RavenJObject LoadMappedResult(Slice key, RavenJObject value, Index dataIndex)
		{
			var reduceKey = value.Value<string>("reduceKey");

			using (var read = dataIndex.Read(this.Snapshot, key))
			{
				if (read == null)
					return null;

				using (var stream = this.documentCodecs.Aggregate(read.Stream, (ds, codec) => codec.Decode(reduceKey, null, ds)))
					return stream.ToJObject();
			}
		}

		public IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(string view, int start, int take)
		{
			var reduceKeyTypesByView = this.tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);
			using (var iterator = reduceKeyTypesByView.MultiRead(this.Snapshot, view))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.ReduceKeyTypes, iterator.CurrentKey, out version);

					yield return new ReduceTypePerKey(value.Value<string>("reduceKey"), (ReduceType)value.Value<int>("reduceType"));

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		private void DeleteScheduledReduction(Slice id, string view, int level, string reduceKey)
		{
			var scheduledReductionsByView = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			var scheduledReductionsByViewAndLevel = this.tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);

			this.tableStorage.ScheduledReductions.Delete(this.writeBatch, id);
			scheduledReductionsByView.MultiDelete(this.writeBatch, view, id);
			scheduledReductionsByViewAndLevelAndReduceKey.MultiDelete(this.writeBatch, this.CreateKey(view, level, reduceKey), id);
			scheduledReductionsByViewAndLevel.MultiDelete(this.writeBatch, this.CreateKey(view, level), id);
		}

		private void DeleteMappedResult(Slice id, string view, string documentId, string reduceKey, string bucket)
		{
			var viewAndDocumentId = this.CreateKey(view, documentId);
			var viewAndReduceKey = this.CreateKey(view, reduceKey);
			var viewAndReduceKeyAndSourceBucket = this.CreateKey(view, reduceKey, bucket);
			var mappedResultsByViewAndDocumentId = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			var mappedResultsByView = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			var mappedResultsByViewAndReduceKey = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsByViewAndReduceKeyAndSourceBucket = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
			var mappedResultsData = this.tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			this.tableStorage.MappedResults.Delete(this.writeBatch, id);
			mappedResultsByViewAndDocumentId.MultiDelete(this.writeBatch, viewAndDocumentId, id);
			mappedResultsByView.MultiDelete(this.writeBatch, view, id);
			mappedResultsByViewAndReduceKey.MultiDelete(this.writeBatch, viewAndReduceKey, id);
			mappedResultsByViewAndReduceKeyAndSourceBucket.MultiDelete(this.writeBatch, viewAndReduceKeyAndSourceBucket, id);
			mappedResultsData.Delete(this.writeBatch, id);
		}

		private void RemoveReduceResult(Slice id, string view, int level, string reduceKey, int sourceBucket)
		{
			var viewAndReduceKeyAndLevelAndSourceBucket = this.CreateKey(view, reduceKey, level, sourceBucket);
			var viewAndReduceKeyAndLevel = this.CreateKey(view, reduceKey, level);
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
				this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
			var reduceResultsByViewAndReduceKeyAndLevel =
				this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
			var reduceResultsData = this.tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			this.tableStorage.ReduceResults.Delete(this.writeBatch, id);
			reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiDelete(this.writeBatch, viewAndReduceKeyAndLevelAndSourceBucket, id);
			reduceResultsByViewAndReduceKeyAndLevel.MultiDelete(this.writeBatch, viewAndReduceKeyAndLevel, id);
			reduceResultsData.Delete(this.writeBatch, id);
		}
	}
}