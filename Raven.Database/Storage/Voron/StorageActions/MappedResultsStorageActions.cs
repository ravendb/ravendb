using System.Text;
using System.Threading;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Util.Streams;

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

		private readonly Reference<WriteBatch> writeBatch;

		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;

        public MappedResultsStorageActions(TableStorage tableStorage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, SnapshotReader snapshot, Reference<WriteBatch> writeBatch, IBufferPool bufferPool)
			: base(snapshot, bufferPool)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
			this.writeBatch = writeBatch;
		}

		public IEnumerable<ReduceKeyAndCount> GetKeysStats(int view, int start, int pageSize)
		{
			var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);
			using (var iterator = reduceKeyCountsByView.MultiRead(Snapshot, CreateLowercasedKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadJson(tableStorage.ReduceKeyCounts, iterator.CurrentKey, writeBatch.Value, out version);

					yield return new ReduceKeyAndCount
					{
						Count = value.Value<int>("mappedItemsCount"),
						Key = value.Value<string>("reduceKey")
					};

					count++;
				}
				while (iterator.MoveNext() && count < pageSize);
			}
		}

		public void PutMappedResult(int view, string docId, string reduceKey, RavenJObject data)
		{
			var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			var mappedResultsByViewAndHashedReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKey);
			var mappedResultsByViewAndHashedReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKeyAndSourceBucket);

			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            var ms = CreateStream();
			using (var stream = documentCodecs.Aggregate((Stream) new UndisposableStream(ms), (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
			{
				data.WriteTo(stream);
				stream.Flush();
			}

			var id = generator.CreateSequentialUuid(UuidType.MappedResults);
			var idAsString = id.ToString();
			var bucket = IndexingUtil.MapBucket(docId);

			var hashedReduceKey = HashReduceKey(reduceKey);
			tableStorage.MappedResults.Add(
				writeBatch.Value,
				idAsString,
				new RavenJObject
				{
					{ "view", view },
					{ "reduceKey", reduceKey },
					{ "hashedReduceKey", hashedReduceKey },
					{ "docId", docId },
					{ "etag", id.ToByteArray() },
					{ "bucket", bucket },
					{ "timestamp", SystemTime.UtcNow }
				}, 0);

			ms.Position = 0;
			mappedResultsData.Add(writeBatch.Value, idAsString, ms, 0);

			mappedResultsByViewAndDocumentId.MultiAdd(writeBatch.Value, CreateLowercasedKey(view, docId), idAsString);
			mappedResultsByView.MultiAdd(writeBatch.Value, CreateLowercasedKey(view), idAsString);
			mappedResultsByViewAndHashedReduceKey.MultiAdd(writeBatch.Value, CreateKey(view, hashedReduceKey), idAsString);
			mappedResultsByViewAndHashedReduceKeyAndSourceBucket.MultiAdd(writeBatch.Value, CreateKey(view, hashedReduceKey, bucket), idAsString);
		}

		public void IncrementReduceKeyCounter(int view, string reduceKey, int val)
		{
			var hashedReduceKey = HashReduceKey(reduceKey);
			var key = CreateKey(view, hashedReduceKey);

			ushort version;
			var value = LoadJson(tableStorage.ReduceKeyCounts, key, writeBatch.Value, out version);

			var newValue = val;
			if (value != null)
				newValue += value.Value<int>("mappedItemsCount");

			AddReduceKeyCount(key, view, reduceKey, hashedReduceKey, newValue, version);
		}

		private void DecrementReduceKeyCounter(int view, string reduceKey, int val)
		{
			var hashedReduceKey = HashReduceKey(reduceKey);
			var key = CreateKey(view, hashedReduceKey);

			ushort reduceKeyCountVersion;
			var reduceKeyCount = LoadJson(tableStorage.ReduceKeyCounts, key, writeBatch.Value, out reduceKeyCountVersion);

			var newValue = -val;
			if (reduceKeyCount != null)
			{
				var currentValue = reduceKeyCount.Value<int>("mappedItemsCount");
				if (currentValue == val)
				{
					var reduceKeyTypeVersion = tableStorage.ReduceKeyTypes.ReadVersion(Snapshot, key);

					DeleteReduceKeyCount(key, view, reduceKeyCountVersion);
					DeleteReduceKeyType(key, view, reduceKeyTypeVersion);
					return;
				}

				newValue += currentValue;
			}

			AddReduceKeyCount(key, view, reduceKey, hashedReduceKey, newValue, reduceKeyCountVersion);
		}

		public void DeleteMappedResultsForDocumentId(string documentId, int view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			var viewAndDocumentId = CreateLowercasedKey(view, documentId);

			var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			using (var iterator = mappedResultsByViewAndDocumentId.MultiRead(Snapshot, viewAndDocumentId))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey.Clone();

					ushort version;
					var value = LoadJson(tableStorage.MappedResults, id, writeBatch.Value, out version);
					var reduceKey = value.Value<string>("reduceKey");
					var bucket = value.Value<int>("bucket");

					DeleteMappedResult(id, view, documentId, reduceKey, bucket.ToString(CultureInfo.InvariantCulture));

					var reduceKeyAndBucket = new ReduceKeyAndBucket(bucket, reduceKey);
					removed[reduceKeyAndBucket] = removed.GetOrDefault(reduceKeyAndBucket) + 1;
				}
				while (iterator.MoveNext());
			}
		}

		public void UpdateRemovedMapReduceStats(int view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			foreach (var keyAndBucket in removed)
			{
				DecrementReduceKeyCounter(view, keyAndBucket.Key.ReduceKey, keyAndBucket.Value);
			}
		}

		public void DeleteMappedResultsForView(int view)
		{
			var deletedReduceKeys = new List<string>();
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);

			using (var iterator = mappedResultsByView.MultiRead(Snapshot, CreateLowercasedKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey.Clone();

					ushort version;
					var value = LoadJson(tableStorage.MappedResults, id, writeBatch.Value, out version);
					var reduceKey = value.Value<string>("reduceKey");
					var bucket = value.Value<string>("bucket");

					DeleteMappedResult(id, view, value.Value<string>("docId"), reduceKey, bucket);

					deletedReduceKeys.Add(reduceKey);
				}
				while (iterator.MoveNext());
			}

			foreach (var g in deletedReduceKeys.GroupBy(x => x, StringComparer.InvariantCultureIgnoreCase))
			{
				DecrementReduceKeyCounter(view, g.Key, g.Count());
			}
		}

		public IEnumerable<string> GetKeysForIndexForDebug(int view, int start, int take)
		{
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			using (var iterator = mappedResultsByView.MultiRead(Snapshot, CreateLowercasedKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<string>();

				var results = new List<string>();
				do
				{
					ushort version;
					var value = LoadJson(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);

					results.Add(value.Value<string>("reduceKey"));
				}
				while (iterator.MoveNext());

				return results
					.Distinct()
					.Skip(start)
					.Take(take);
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(int view, string key, int start, int take)
		{
			var viewAndHashedReduceKey = CreateKey(view, HashReduceKey(key));
			var mappedResultsByViewAndHashedReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKey);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndHashedReduceKey.MultiRead(Snapshot, viewAndHashedReduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadJson(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
					var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);
					yield return new MappedResultInfo
					{
						ReduceKey = value.Value<string>("reduceKey"),
						Etag = Etag.Parse(value.Value<byte[]>("etag")),
						Timestamp = value.Value<DateTime>("timestamp"),
						Bucket = value.Value<int>("bucket"),
						Source = value.Value<string>("docId"),
						Size = size,
						Data = LoadMappedResult(iterator.CurrentKey, value, mappedResultsData)
					};

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(int view, string reduceKey, int level, int start, int take)
		{
			var viewAndHashedReduceKeyAndLevel = CreateKey(view, HashReduceKey(reduceKey), level);
			var reduceResultsByViewAndHashedReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevel);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			using (var iterator = reduceResultsByViewAndHashedReduceKeyAndLevel.MultiRead(Snapshot, viewAndHashedReduceKeyAndLevel))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadJson(tableStorage.ReduceResults, iterator.CurrentKey, writeBatch.Value, out version);
					var size = tableStorage.ReduceResults.GetDataSize(Snapshot, iterator.CurrentKey);

					yield return
						new MappedResultInfo
						{
							ReduceKey = value.Value<string>("reduceKey"),
							Etag = Etag.Parse(value.Value<byte[]>("etag")),
							Timestamp = value.Value<DateTime>("timestamp"),
							Bucket = value.Value<int>("bucket"),
							Source = value.Value<string>("sourceBucket"),
							Size = size,
							Data = LoadMappedResult(iterator.CurrentKey, value, reduceResultsData)
						};

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(int view, int start, int take)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, CreateLowercasedKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadJson(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);

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

		public void ScheduleReductions(int view, int level, ReduceKeyAndBucket reduceKeysAndBuckets)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndHashedReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndHashedReduceKey);
			var scheduledReductionsByViewAndLevel = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);

			var id = generator.CreateSequentialUuid(UuidType.ScheduledReductions);
			var idAsString = id.ToString();

			tableStorage.ScheduledReductions.Add(writeBatch.Value, idAsString, new RavenJObject
			{
				{"view", view},
				{"reduceKey", reduceKeysAndBuckets.ReduceKey},
				{"hashed_reduce_key", HashReduceKey(reduceKeysAndBuckets.ReduceKey)},
				{"bucket", reduceKeysAndBuckets.Bucket},
				{"level", level},
				{"etag", id.ToByteArray()},
				{"timestamp", SystemTime.UtcNow}
			});

			scheduledReductionsByView.MultiAdd(writeBatch.Value, CreateLowercasedKey(view), idAsString);
			scheduledReductionsByViewAndLevelAndHashedReduceKey.MultiAdd(writeBatch.Value, CreateKey(view, level, reduceKeysAndBuckets.ReduceKey), idAsString);
			scheduledReductionsByViewAndLevel.MultiAdd(writeBatch.Value, CreateLowercasedKey(view, level), idAsString);
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams)
		{
			var scheduledReductionsByViewAndLevelAndHashedReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndHashedReduceKey);

			var seenLocally = new HashSet<Tuple<string, int>>();
			foreach (var reduceKey in getItemsToReduceParams.ReduceKeys.ToArray())
			{
				var viewAndLevelAndHashedReduceKey = CreateKey(getItemsToReduceParams.Index, getItemsToReduceParams.Level, HashReduceKey(reduceKey));
				using (var iterator = scheduledReductionsByViewAndLevelAndHashedReduceKey.MultiRead(Snapshot, viewAndLevelAndHashedReduceKey))
				{
					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						if (getItemsToReduceParams.Take <= 0)
							break;

						ushort version;
						var value = LoadJson(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);

						var indexFromDb = value.Value<int>("view");
						var levelFromDb = value.Value<int>("level");
						var reduceKeyFromDb = value.Value<string>("reduceKey");

						if (indexFromDb != getItemsToReduceParams.Index || levelFromDb != getItemsToReduceParams.Level)
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

		private IEnumerable<MappedResultInfo> GetResultsForBucket(int view, int level, string reduceKey, int bucket, bool loadData)
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

		private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(int view, string reduceKey, int level, int bucket, bool loadData)
		{
			var viewAndHashedReduceKeyAndLevelAndBucket = CreateKey(view, HashReduceKey(reduceKey), level, bucket);

			var reduceResultsByViewAndHashedReduceKeyAndLevelAndBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevelAndBucket);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);
			using (var iterator = reduceResultsByViewAndHashedReduceKeyAndLevelAndBucket.MultiRead(Snapshot, viewAndHashedReduceKeyAndLevelAndBucket))
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
					var value = LoadJson(tableStorage.ReduceResults, iterator.CurrentKey, writeBatch.Value, out version);
					var size = tableStorage.ReduceResults.GetDataSize(Snapshot, iterator.CurrentKey);

					yield return new MappedResultInfo
					{
						ReduceKey = value.Value<string>("reduceKey"),
						Etag = Etag.Parse(value.Value<byte[]>("etag")),
						Timestamp = value.Value<DateTime>("timestamp"),
						Bucket = value.Value<int>("bucket"),
						Source = null,
						Size = size,
						Data = loadData ? LoadMappedResult(iterator.CurrentKey, value, reduceResultsData) : null
					};
				}
				while (iterator.MoveNext());
			}
		}

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(int view, string reduceKey, int bucket, bool loadData)
		{
			var viewAndHashedReduceKeyAndSourceBucket = CreateKey(view, HashReduceKey(reduceKey), bucket);

			var mappedResultsByViewAndHashedReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKeyAndSourceBucket);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndHashedReduceKeyAndSourceBucket.MultiRead(Snapshot, viewAndHashedReduceKeyAndSourceBucket))
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
					var value = LoadJson(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
					var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);

					yield return new MappedResultInfo
					{
						ReduceKey = value.Value<string>("reduceKey"),
						Etag = Etag.Parse(value.Value<byte[]>("etag")),
						Timestamp = value.Value<DateTime>("timestamp"),
						Bucket = value.Value<int>("bucket"),
						Source = null,
						Size = size,
						Data = loadData ? LoadMappedResult(iterator.CurrentKey, value, mappedResultsData) : null
					};
				}
				while (iterator.MoveNext());
			}
		}

		public ScheduledReductionInfo DeleteScheduledReduction(IEnumerable<object> itemsToDelete)
		{
			if (itemsToDelete == null)
				return null;

			var result = new ScheduledReductionInfo();
			var hasResult = false;
			var currentEtag = Etag.Empty;
			foreach (RavenJToken token in itemsToDelete)
			{
				var etag = Etag.Parse(token.Value<byte[]>("etag"));
				var etagAsString = etag.ToString();

				ushort version;
				var value = LoadJson(tableStorage.ScheduledReductions, etagAsString, writeBatch.Value, out version);
				if (value == null)
					continue;

				if (etag.CompareTo(currentEtag) > 0)
				{
					hasResult = true;
					result.Etag = etag;
					result.Timestamp = value.Value<DateTime>("timestamp");
				}

				var view = value.Value<int>("view");
				var level = value.Value<int>("level");
				var reduceKey = value.Value<string>("reduceKey");

				DeleteScheduledReduction(etagAsString, view, level, reduceKey);
			}

			return hasResult ? result : null;
		}

		public void DeleteScheduledReduction(int view, int level, string reduceKey)
		{
			var scheduledReductionsByViewAndLevelAndHashedReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndHashedReduceKey);
			using (var iterator = scheduledReductionsByViewAndLevelAndHashedReduceKey.MultiRead(Snapshot, CreateKey(view, level, HashReduceKey(reduceKey))))
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

		private static readonly ThreadLocal<IHashEncryptor> localSha1 = new ThreadLocal<IHashEncryptor>(() => Encryptor.Current.CreateHash());

		public static byte[] HashReduceKey(string reduceKey)
		{
			return localSha1.Value.Compute20(Encoding.UTF8.GetBytes(reduceKey));
		}

		public void PutReducedResult(int view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			var reduceResultsByViewAndHashedReduceKeyAndLevelAndSourceBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevelAndSourceBucket);
			var reduceResultsByViewAndHashedReduceKeyAndLevelAndBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevelAndBucket);
			var reduceResultsByViewAndHashedReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevel);
			var reduceResultsByView =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

            var ms = CreateStream();
			using (
				var stream = documentCodecs.Aggregate((Stream) new UndisposableStream(ms),
					(ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
			{
				data.WriteTo(stream);
				stream.Flush();
			}

			var id = generator.CreateSequentialUuid(UuidType.MappedResults);
			var idAsString = id.ToString();

			var hashedReduceKey = HashReduceKey(reduceKey);
			tableStorage.ReduceResults.Add(
				writeBatch.Value,
				idAsString,
				new RavenJObject
				{
					{ "view", view },
					{ "etag", id.ToByteArray() },
					{ "reduceKey", reduceKey },
					{ "hashedReduceKey", hashedReduceKey },
					{ "level", level },
					{ "sourceBucket", sourceBucket },
					{ "bucket", bucket },
					{ "timestamp", SystemTime.UtcNow }
				},
				0);

			ms.Position = 0;
			reduceResultsData.Add(writeBatch.Value, idAsString, ms, 0);

			var viewAndHashedReduceKeyAndLevelAndSourceBucket = CreateKey(view, hashedReduceKey, level, sourceBucket);
			var viewAndHashedReduceKeyAndLevel = CreateKey(view, hashedReduceKey, level);
			var viewAndHashedReduceKeyAndLevelAndBucket = CreateKey(view, hashedReduceKey, level, bucket);

			reduceResultsByViewAndHashedReduceKeyAndLevelAndSourceBucket.MultiAdd(writeBatch.Value, viewAndHashedReduceKeyAndLevelAndSourceBucket, idAsString);
			reduceResultsByViewAndHashedReduceKeyAndLevel.MultiAdd(writeBatch.Value, viewAndHashedReduceKeyAndLevel, idAsString);
			reduceResultsByViewAndHashedReduceKeyAndLevelAndBucket.MultiAdd(writeBatch.Value, viewAndHashedReduceKeyAndLevelAndBucket, idAsString);
			reduceResultsByView.MultiAdd(writeBatch.Value, CreateLowercasedKey(view), idAsString);
		}

		public void RemoveReduceResults(int view, int level, string reduceKey, int sourceBucket)
		{
			var viewAndHashedReduceKeyAndLevelAndSourceBucket = CreateKey(view, HashReduceKey(reduceKey), level, sourceBucket);
			var reduceResultsByViewAndHashedReduceKeyAndLevelAndSourceBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevelAndSourceBucket);

			using (var iterator = reduceResultsByViewAndHashedReduceKeyAndLevelAndSourceBucket.MultiRead(Snapshot, viewAndHashedReduceKeyAndLevelAndSourceBucket))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					RemoveReduceResult(iterator.CurrentKey.Clone());
				}
				while (iterator.MoveNext());
			}
		}

		public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(int view, int take, int limitOfItemsToReduceInSingleStep)
		{
			if (take <= 0)
				take = 1;

			var allKeysToReduce = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

			var viewAndLevel = CreateLowercasedKey(view, 0);
			var scheduledReductionsByViewAndLevel = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);
			using (var iterator = scheduledReductionsByViewAndLevel.MultiRead(Snapshot, viewAndLevel))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return Enumerable.Empty<ReduceTypePerKey>();

				do
				{
					ushort version;
					var value = LoadJson(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);

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

			return reduceTypesPerKeys
				.Select(x => new ReduceTypePerKey(x.Key, x.Value))
				.Take(take);
		}

		private int GetNumberOfMappedItemsPerReduceKey(int view, string reduceKey)
		{
			var key = CreateKey(view, HashReduceKey(reduceKey));

			ushort version;
			var value = LoadJson(tableStorage.ReduceKeyCounts, key, writeBatch.Value, out version);
			if (value == null)
				return 0;

			return value.Value<int>("mappedItemsCount");
		}

		public void UpdatePerformedReduceType(int view, string reduceKey, ReduceType reduceType)
		{
			var key = CreateKey(view, HashReduceKey(reduceKey));
			var version = tableStorage.ReduceKeyTypes.ReadVersion(Snapshot, key);

			AddReduceKeyType(key, view, reduceKey, reduceType, version);
		}

		private void DeleteReduceKeyCount(string key, int view, ushort? expectedVersion)
		{
			var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);

			tableStorage.ReduceKeyCounts.Delete(writeBatch.Value, key, expectedVersion);
			reduceKeyCountsByView.MultiDelete(writeBatch.Value, CreateLowercasedKey(view), key);
		}

		private void DeleteReduceKeyType(string key, int view, ushort? expectedVersion)
		{
			var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

			tableStorage.ReduceKeyTypes.Delete(writeBatch.Value, key, expectedVersion);
			reduceKeyTypesByView.MultiDelete(writeBatch.Value, CreateLowercasedKey(view), key);
		}

		private void AddReduceKeyCount(string key, int view, string reduceKey, byte[] hashedReduceKey, int count, ushort? expectedVersion)
		{
			var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);

			tableStorage.ReduceKeyCounts.Add(
						writeBatch.Value,
						key,
						new RavenJObject
						{
							{ "view", view },
							{ "reduceKey", reduceKey },
							{ "hashedReduceKey", hashedReduceKey },
							{ "mappedItemsCount", count }
						}, expectedVersion);

			reduceKeyCountsByView.MultiAdd(writeBatch.Value, CreateLowercasedKey(view), key);
		}

		private void AddReduceKeyType(string key, int view, string reduceKey, ReduceType status, ushort? expectedVersion)
		{
			var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

			tableStorage.ReduceKeyTypes.Add(
						writeBatch.Value,
						key,
						new RavenJObject
						{
							{ "view", view },
							{ "reduceKey", reduceKey },
							{ "reduceType", (int)status }
						}, expectedVersion);

			reduceKeyTypesByView.MultiAdd(writeBatch.Value, CreateLowercasedKey(view), key);
		}

		public ReduceType GetLastPerformedReduceType(int view, string reduceKey)
		{
			var key = CreateKey(view, HashReduceKey(reduceKey));

			ushort version;
			var value = LoadJson(tableStorage.ReduceKeyTypes, key, writeBatch.Value, out version);
			if (value == null)
				return ReduceType.None;

			return (ReduceType)value.Value<int>("reduceType");
		}

		public IEnumerable<int> GetMappedBuckets(int view, string reduceKey)
		{
			var viewAndHashedReduceKey = CreateKey(view, HashReduceKey(reduceKey));
			var mappedResultsByViewAndHashedReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKey);

			using (var iterator = mappedResultsByViewAndHashedReduceKey.MultiRead(Snapshot, viewAndHashedReduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					yield break;

				do
				{
					ushort version;
					var value = LoadJson(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);

					yield return value.Value<int>("bucket");
				}
				while (iterator.MoveNext());
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResults(int view, IEnumerable<string> keysToReduce, bool loadData)
		{
			var mappedResultsByViewAndHashedReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKey);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			foreach (var reduceKey in keysToReduce)
			{
				var viewAndHashedReduceKey = CreateKey(view, HashReduceKey(reduceKey));
				using (var iterator = mappedResultsByViewAndHashedReduceKey.MultiRead(Snapshot, viewAndHashedReduceKey))
				{
					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						ushort version;
						var value = LoadJson(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
						var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);

						var hashedReduceKeyFromDb = value.Value<byte[]>("hashedReduceKey");
						var hashedReduceKey = HashReduceKey(reduceKey);
						if (hashedReduceKey.SequenceEqual(hashedReduceKeyFromDb) == false)
						{
							break;
						}

						yield return new MappedResultInfo
						{
							Bucket = value.Value<int>("bucket"),
							ReduceKey = value.Value<string>("reduceKey"),
							Etag = Etag.Parse(value.Value<byte[]>("etag")),
							Timestamp = value.Value<DateTime>("timestamp"),
							Data = loadData ? LoadMappedResult(iterator.CurrentKey, value, mappedResultsData) : null,
							Size = size
						};
					}
					while (iterator.MoveNext());
				}
			}
		}

		private RavenJObject LoadMappedResult(Slice key, RavenJObject value, Index dataIndex)
		{
			var reduceKey = value.Value<string>("reduceKey");

			var read = dataIndex.Read(Snapshot, key, writeBatch.Value);
			if (read == null)
				return null;

			using (var readerStream = read.Reader.AsStream())
			{
				using (var stream = documentCodecs.Aggregate(readerStream, (ds, codec) => codec.Decode(reduceKey, null, ds)))
					return stream.ToJObject();
			}
		}

		public IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(int view, int start, int take)
		{
			var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);
			using (var iterator = reduceKeyTypesByView.MultiRead(Snapshot, CreateLowercasedKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadJson(tableStorage.ReduceKeyTypes, iterator.CurrentKey, writeBatch.Value, out version);

					yield return new ReduceTypePerKey(value.Value<string>("reduceKey"), (ReduceType)value.Value<int>("reduceType"));

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public void DeleteScheduledReductionForView(int view)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);

			using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, CreateLowercasedKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey.Clone();

					ushort version;
					var value = LoadJson(tableStorage.ScheduledReductions, id, writeBatch.Value, out version);
					if (value == null)
						continue;

					var v = value.Value<int>("view");
					var level = value.Value<int>("level");
					var reduceKey = value.Value<string>("reduceKey");

					DeleteScheduledReduction(id, v, level, reduceKey);
				}
				while (iterator.MoveNext());
			}
		}

		public void RemoveReduceResultsForView(int view)
		{
			var reduceResultsByView =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);

			using (var iterator = reduceResultsByView.MultiRead(Snapshot, CreateLowercasedKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey.Clone();

					RemoveReduceResult(id);
				}
				while (iterator.MoveNext());
			}
		}

		private void DeleteScheduledReduction(Slice id, int view, int level, string reduceKey)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndHashedReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndHashedReduceKey);
			var scheduledReductionsByViewAndLevel = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevel);

			tableStorage.ScheduledReductions.Delete(writeBatch.Value, id);
			scheduledReductionsByView.MultiDelete(writeBatch.Value, CreateLowercasedKey(view), id);
			scheduledReductionsByViewAndLevelAndHashedReduceKey.MultiDelete(writeBatch.Value, CreateKey(view, level, HashReduceKey(reduceKey)), id);
			scheduledReductionsByViewAndLevel.MultiDelete(writeBatch.Value, CreateLowercasedKey(view, level), id);
		}

		private void DeleteMappedResult(Slice id, int view, string documentId, string reduceKey, string bucket)
		{
			var viewAndDocumentId = CreateLowercasedKey(view, documentId);
			var hashedReduceKey = HashReduceKey(reduceKey);
			var viewAndHashedReduceKey = CreateKey(view, hashedReduceKey);
			var viewAndHashedReduceKeyAndSourceBucket = CreateKey(view, hashedReduceKey, bucket);
			var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
			var mappedResultsByViewAndHashedReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKey);
			var mappedResultsByViewAndHashedReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndHashedReduceKeyAndSourceBucket);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			tableStorage.MappedResults.Delete(writeBatch.Value, id);
			mappedResultsByViewAndDocumentId.MultiDelete(writeBatch.Value, viewAndDocumentId, id);
			mappedResultsByView.MultiDelete(writeBatch.Value, CreateLowercasedKey(view), id);
			mappedResultsByViewAndHashedReduceKey.MultiDelete(writeBatch.Value, viewAndHashedReduceKey, id);
			mappedResultsByViewAndHashedReduceKeyAndSourceBucket.MultiDelete(writeBatch.Value, viewAndHashedReduceKeyAndSourceBucket, id);
			mappedResultsData.Delete(writeBatch.Value, id);
		}

		private void RemoveReduceResult(Slice id)
		{
			ushort version;
			var value = LoadJson(tableStorage.ReduceResults, id, writeBatch.Value, out version);

			var view = value.Value<string>("view");
			var hashedReduceKey = value.Value<string>("hashedReduceKey");
			var level = value.Value<int>("level");
			var bucket = value.Value<int>("bucket");
			var sourceBucket = value.Value<int>("sourceBucket");

			var viewAndHashedReduceKeyAndLevelAndSourceBucket = CreateKey(view, hashedReduceKey, level, sourceBucket);
			var viewAndHashedReduceKeyAndLevel = CreateKey(view, hashedReduceKey, level);
			var viewAndHashedReduceKeyAndLevelAndBucket = CreateKey(view, hashedReduceKey, level, bucket);

			var reduceResultsByViewAndHashedReduceKeyAndLevelAndSourceBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevelAndSourceBucket);
			var reduceResultsByViewAndHashedReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevel);
			var reduceResultsByViewAndHashedReduceKeyAndLevelAndBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndHashedReduceKeyAndLevelAndBucket);
			var reduceResultsByView =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			tableStorage.ReduceResults.Delete(writeBatch.Value, id);
			reduceResultsByViewAndHashedReduceKeyAndLevelAndSourceBucket.MultiDelete(writeBatch.Value, viewAndHashedReduceKeyAndLevelAndSourceBucket, id);
			reduceResultsByViewAndHashedReduceKeyAndLevel.MultiDelete(writeBatch.Value, viewAndHashedReduceKeyAndLevel, id);
			reduceResultsByViewAndHashedReduceKeyAndLevelAndBucket.MultiDelete(writeBatch.Value, viewAndHashedReduceKeyAndLevelAndBucket, id);
			reduceResultsByView.MultiDelete(writeBatch.Value, CreateLowercasedKey(view), id);
			reduceResultsData.Delete(writeBatch.Value, id);
		}
	}
}