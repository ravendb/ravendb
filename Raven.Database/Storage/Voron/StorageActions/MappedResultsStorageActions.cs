using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Raven.Database.Util;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;
	using System.Collections.Generic;
	using System.Globalization;
	using System.IO;
	using System.Linq;

	using Abstractions;
	using Abstractions.Data;
	using Abstractions.Extensions;
	using Abstractions.MEF;
	using Database.Impl;
	using Indexing;
	using Plugins;
	using Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	using Index = Raven.Database.Storage.Voron.Impl.Index;

	internal class MappedResultsStorageActions : StorageActionsBase, IMappedResultsStorageAction
	{
		private readonly TableStorage tableStorage;

		private readonly IUuidGenerator generator;

		private readonly Reference<WriteBatch> writeBatch;
		private readonly IStorageActionsAccessor storageActionsAccessor;

		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;

        public MappedResultsStorageActions(TableStorage tableStorage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, Reference<SnapshotReader> snapshot, Reference<WriteBatch> writeBatch, IBufferPool bufferPool, IStorageActionsAccessor storageActionsAccessor)
			: base(snapshot, bufferPool)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
			this.writeBatch = writeBatch;
	        this.storageActionsAccessor = storageActionsAccessor;
		}

		public IEnumerable<ReduceKeyAndCount> GetKeysStats(int view, int start, int pageSize)
		{
			var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);
            using (var iterator = reduceKeyCountsByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.ReduceKeyCounts, iterator.CurrentKey, writeBatch.Value, out version);

					Debug.Assert(value != null);
					yield return new ReduceKeyAndCount
					{
						Count = value.ReadInt(ReduceKeyCountFields.MappedItemsCount),
						Key = value.ReadString(ReduceKeyCountFields.ReduceKey)
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
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);

			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            var ms = CreateStream();
			using (var stream = documentCodecs.Aggregate((Stream) new UndisposableStream(ms), (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
			{
				data.WriteTo(stream);
				stream.Flush();
			}

			var id = generator.CreateSequentialUuid(UuidType.MappedResults);
            var idSlice = (Slice)id.ToString();
			var bucket = IndexingUtil.MapBucket(docId);

		    var reduceKeyHash = HashKey(reduceKey);

			var mappedResult = new Structure<MappedResultFields>(tableStorage.MappedResults.Schema)
				.Set(MappedResultFields.IndexId, view)
				.Set(MappedResultFields.Bucket, bucket)
				.Set(MappedResultFields.Timestamp, SystemTime.UtcNow.ToBinary())
				.Set(MappedResultFields.ReduceKey, reduceKey)
				.Set(MappedResultFields.DocId, docId)
				.Set(MappedResultFields.Etag, id.ToByteArray());

			tableStorage.MappedResults.AddStruct(
				writeBatch.Value,
                idSlice,
				mappedResult, 0);

			ms.Position = 0;
            mappedResultsData.Add(writeBatch.Value, idSlice, ms, 0);

            string viewKey = CreateKey(view);
			string viewReduceKey = AppendToKey(viewKey, ReduceKeySizeLimited(reduceKey));
            string viewReduceHashKey = AppendToKey(viewReduceKey, reduceKeyHash);

            mappedResultsByViewAndDocumentId.MultiAdd(writeBatch.Value, (Slice)AppendToKey(viewKey, docId), idSlice);
            mappedResultsByView.MultiAdd(writeBatch.Value, (Slice)viewKey, idSlice);
            mappedResultsByViewAndReduceKey.MultiAdd(writeBatch.Value, (Slice)viewReduceHashKey, idSlice);
            mappedResultsByViewAndReduceKeyAndSourceBucket.MultiAdd(writeBatch.Value, (Slice)AppendToKey(viewReduceHashKey, bucket), idSlice);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static string ReduceKeySizeLimited(string key)
		{
			if (key.Length < 512)
				return key;
			return key.Substring(0, 500) + "<truncated>";
		}

        public void IncrementReduceKeyCounter(int view, string reduceKey, int val)
		{
		    var reduceKeyHash = HashKey(reduceKey);
            var viewKey = CreateKey(view);            
            
            var keySlice = (Slice)AppendToKey(viewKey, ReduceKeySizeLimited(reduceKey), reduceKeyHash);
            var viewKeySlice = (Slice)viewKey;

			ushort version;
            var value = LoadStruct(tableStorage.ReduceKeyCounts, keySlice, writeBatch.Value, out version);

			var newValue = val;
			if (value != null)
				newValue += value.ReadInt(ReduceKeyCountFields.MappedItemsCount);

            AddReduceKeyCount(keySlice, view, viewKeySlice, reduceKey, newValue, version);
		}

        private void DecrementReduceKeyCounter(int view, string viewKey, string reduceKey, int val)
		{
            var reduceKeyHash = HashKey(reduceKey);
            var key =  AppendToKey(viewKey, reduceKey, reduceKeyHash);
            var keySlice = (Slice)key;
            var viewKeySlice = (Slice)viewKey;

			ushort reduceKeyCountVersion;
            var reduceKeyCount = LoadStruct(tableStorage.ReduceKeyCounts, keySlice, writeBatch.Value, out reduceKeyCountVersion);

			var newValue = -val;
			if (reduceKeyCount != null)
			{
				var currentValue = reduceKeyCount.ReadInt(ReduceKeyCountFields.MappedItemsCount);
				if (currentValue == val)
				{
                    var reduceKeyTypeVersion = tableStorage.ReduceKeyTypes.ReadVersion(Snapshot, keySlice, writeBatch.Value);

                    DeleteReduceKeyCount(keySlice, view, viewKeySlice, reduceKeyCountVersion);
                    DeleteReduceKeyType(keySlice, view, viewKeySlice, reduceKeyTypeVersion);
					return;
				}

				newValue += currentValue;
			}

            AddReduceKeyCount(keySlice, view, viewKeySlice, reduceKey, newValue, reduceKeyCountVersion);
		}

		public void DeleteMappedResultsForDocumentId(string documentId, int view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
            var viewKey = CreateKey(view);
            var viewKeySlice = new Slice(viewKey);
			var viewAndDocumentId = new Slice(AppendToKey(viewKey, documentId));

            var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndDocumentId.MultiRead(Snapshot, viewAndDocumentId))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					// TODO: Check if we can relax the clone.
                    var id = iterator.CurrentKey.Clone();

					ushort version;
					var value = LoadStruct(tableStorage.MappedResults, id, writeBatch.Value, out version);
					var reduceKey = value.ReadString(MappedResultFields.ReduceKey);
					var bucket = value.ReadInt(MappedResultFields.Bucket);

                    var reduceKeyHash = HashKey(reduceKey);                  
                    var viewAndReduceKey = AppendToKey(viewKey, ReduceKeySizeLimited(reduceKey), reduceKeyHash);
                    var viewAndReduceKeyAndSourceBucket = AppendToKey(viewAndReduceKey, bucket);

                    tableStorage.MappedResults.Delete(writeBatch.Value, id);

                    mappedResultsByViewAndDocumentId.MultiDelete(writeBatch.Value, viewAndDocumentId, id);
                    mappedResultsByView.MultiDelete(writeBatch.Value, viewKeySlice, id);
                    mappedResultsByViewAndReduceKey.MultiDelete(writeBatch.Value, (Slice)viewAndReduceKey, id);
                    mappedResultsByViewAndReduceKeyAndSourceBucket.MultiDelete(writeBatch.Value, (Slice)viewAndReduceKeyAndSourceBucket, id);
                    mappedResultsData.Delete(writeBatch.Value, id);

					var reduceKeyAndBucket = new ReduceKeyAndBucket(bucket, reduceKey);
					removed[reduceKeyAndBucket] = removed.GetOrDefault(reduceKeyAndBucket) + 1;
				}
				while (iterator.MoveNext());
			}
		}

		public void UpdateRemovedMapReduceStats(int view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
            var viewKey = CreateKey(view);
			foreach (var keyAndBucket in removed)
			{
                DecrementReduceKeyCounter(view, viewKey, keyAndBucket.Key.ReduceKey, keyAndBucket.Value);
			}
		}

		public void DeleteMappedResultsForView(int view)
		{
			var deletedReduceKeys = new List<string>();

            var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            var viewKey = CreateKey(view);

            using (var iterator = mappedResultsByView.MultiRead(Snapshot, (Slice)viewKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey.Clone();

					ushort version;
					var value = LoadStruct(tableStorage.MappedResults, id, writeBatch.Value, out version);
					var reduceKey = value.ReadString(MappedResultFields.ReduceKey);
					var bucket = value.ReadInt(MappedResultFields.Bucket);
					var documentId = value.ReadString(MappedResultFields.DocId);

                    var reduceKeyHash = HashKey(reduceKey);

                    var viewAndDocumentId = AppendToKey(viewKey, documentId);
                    var viewAndReduceKey = AppendToKey(viewKey, ReduceKeySizeLimited(reduceKey), reduceKeyHash);
                    var viewAndReduceKeyAndSourceBucket = AppendToKey(viewAndReduceKey, bucket);

                    tableStorage.MappedResults.Delete(writeBatch.Value, id);

                    mappedResultsByViewAndDocumentId.MultiDelete(writeBatch.Value, (Slice)viewAndDocumentId, id);
                    mappedResultsByView.MultiDelete(writeBatch.Value, (Slice)viewKey, id);
                    mappedResultsByViewAndReduceKey.MultiDelete(writeBatch.Value, (Slice)viewAndReduceKey, id);
                    mappedResultsByViewAndReduceKeyAndSourceBucket.MultiDelete(writeBatch.Value, (Slice)viewAndReduceKeyAndSourceBucket, id);
                    mappedResultsData.Delete(writeBatch.Value, id);
					
					deletedReduceKeys.Add(reduceKey);
					storageActionsAccessor.General.MaybePulseTransaction();
				}
				while (iterator.MoveNext());
			}

			foreach (var g in deletedReduceKeys.GroupBy(x => x, StringComparer.InvariantCultureIgnoreCase))
			{
				DecrementReduceKeyCounter(view, viewKey, g.Key, g.Count());
			}
		}

		public IEnumerable<string> GetKeysForIndexForDebug(int view, string startsWith, string sourceId, int start, int take)
		{
			var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            using (var iterator = mappedResultsByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys)) 
					return Enumerable.Empty<string>();

				var needExactMatch = take == 1;
				var results = new List<string>();
				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);

					if (string.IsNullOrEmpty(sourceId) == false)
					{
						var docId = value.ReadString(MappedResultFields.DocId);
						if (string.Equals(sourceId, docId, StringComparison.OrdinalIgnoreCase) == false)
							continue;
					}

					var reduceKey = value.ReadString(MappedResultFields.ReduceKey);

					if (StringHelper.Compare(startsWith, reduceKey, needExactMatch) == false)
						continue;

					results.Add(reduceKey);
				}
				while (iterator.MoveNext());

				return results
					.Distinct()
					.Skip(start)
					.Take(take);
			}
		}

        public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(int view, string reduceKey, int start, int take)
        {
            var reduceKeyHash = HashKey(reduceKey);
            var viewAndReduceKey = (Slice)CreateKey(view, reduceKey, reduceKeyHash);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(Snapshot, viewAndReduceKey))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
                    var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);
                    yield return new MappedResultInfo
                    {
                        ReduceKey = value.ReadString(MappedResultFields.ReduceKey),
                        Etag = Etag.Parse(value.ReadBytes(MappedResultFields.Etag)),
                        Timestamp = DateTime.FromBinary(value.ReadLong(MappedResultFields.Timestamp)),
                        Bucket = value.ReadInt(MappedResultFields.Bucket),
                        Source = value.ReadString(MappedResultFields.DocId),
                        Size = size,
						Data = LoadMappedResult(iterator.CurrentKey, value.ReadString(MappedResultFields.ReduceKey), mappedResultsData)
                    };

                    count++;
                }
                while (iterator.MoveNext() && count < take);
            }
        }

		public IEnumerable<string> GetSourcesForIndexForDebug(int view, string startsWith, int take)
        {
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            using (var iterator = mappedResultsByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return Enumerable.Empty<string>();

	            var needExactMatch = take == 1;
                var results = new HashSet<string>();
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);

                    var docId = value.ReadString(MappedResultFields.DocId);

					if (StringHelper.Compare(startsWith, docId, needExactMatch) == false)
						continue;

                    results.Add(docId);
                }
                while (iterator.MoveNext() && results.Count <= take);

                return results;
            }
        }

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(int view, string reduceKey, int level, int start, int take)
		{
		    var reduceKeyHash = HashKey(reduceKey);
            var viewAndReduceKeyAndLevel = (Slice) CreateKey(view, ReduceKeySizeLimited(reduceKey), reduceKeyHash, level);
			var reduceResultsByViewAndReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

			using (var iterator = reduceResultsByViewAndReduceKeyAndLevel.MultiRead(Snapshot, viewAndReduceKeyAndLevel))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.ReduceResults, iterator.CurrentKey, writeBatch.Value, out version);
					var size = tableStorage.ReduceResults.GetDataSize(Snapshot, iterator.CurrentKey);

					var readReduceKey = value.ReadString(ReduceResultFields.ReduceKey);

					yield return
						new MappedResultInfo
						{
							ReduceKey = readReduceKey,
							Etag = Etag.Parse(value.ReadBytes(ReduceResultFields.Etag)),
							Timestamp = DateTime.FromBinary(value.ReadLong(ReduceResultFields.Timestamp)),
							Bucket = value.ReadInt(ReduceResultFields.Bucket),
							Source = value.ReadInt(ReduceResultFields.SourceBucket).ToString(),
							Size = size,
							Data = LoadMappedResult(iterator.CurrentKey, readReduceKey, reduceResultsData)
						};

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(int view, int start, int take)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);

					yield return new ScheduledReductionDebugInfo
					{
						Key = value.ReadString(ScheduledReductionFields.ReduceKey),
						Bucket = value.ReadInt(ScheduledReductionFields.Bucket),
						Etag = new Guid(value.ReadBytes(ScheduledReductionFields.Etag)),
						Level = value.ReadInt(ScheduledReductionFields.Level),
						Timestamp = DateTime.FromBinary(value.ReadLong(ScheduledReductionFields.Timestamp)),
					};

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public void ScheduleReductions(int view, int level, ReduceKeyAndBucket reduceKeysAndBuckets)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

			var id = generator.CreateSequentialUuid(UuidType.ScheduledReductions);
            var idSlice = (Slice)id.ToString();
		    var reduceHashKey = HashKey(reduceKeysAndBuckets.ReduceKey);

			var scheduledReduction = new Structure<ScheduledReductionFields>(tableStorage.ScheduledReductions.Schema);

			scheduledReduction.Set(ScheduledReductionFields.IndexId, view)
				.Set(ScheduledReductionFields.ReduceKey, reduceKeysAndBuckets.ReduceKey)
				.Set(ScheduledReductionFields.Bucket, reduceKeysAndBuckets.Bucket)
				.Set(ScheduledReductionFields.Level, level)
				.Set(ScheduledReductionFields.Etag, id.ToByteArray())
				.Set(ScheduledReductionFields.Timestamp, SystemTime.UtcNow.ToBinary());

			tableStorage.ScheduledReductions.AddStruct(writeBatch.Value, idSlice, scheduledReduction);

            var viewKey = CreateKey(view);
            var viewKeySlice = (Slice)viewKey;


            scheduledReductionsByView.MultiAdd(writeBatch.Value, viewKeySlice, idSlice);
            scheduledReductionsByViewAndLevelAndReduceKey.MultiAdd(writeBatch.Value, (Slice)AppendToKey(viewKey, level, ReduceKeySizeLimited(reduceKeysAndBuckets.ReduceKey), reduceHashKey), idSlice);
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams)
		{
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
            var deleter = new ScheduledReductionDeleter(getItemsToReduceParams.ItemsToDelete, o =>
            {
                var etag = o as Etag;
                if (etag == null) 
                    return null;

                return (Slice)etag.ToString();
            });

			var seenLocally = new HashSet<Tuple<string, int>>();
			foreach (var reduceKey in getItemsToReduceParams.ReduceKeys.ToArray())
			{
			    var reduceKeyHash = HashKey(reduceKey);
                var viewAndLevelAndReduceKey = (Slice) CreateKey(getItemsToReduceParams.Index, getItemsToReduceParams.Level, ReduceKeySizeLimited(reduceKey), reduceKeyHash);
				using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(Snapshot, viewAndLevelAndReduceKey))
				{
					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						if (getItemsToReduceParams.Take <= 0)
							break;

						ushort version;
						var value = LoadStruct(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);

						var reduceKeyFromDb = value.ReadString(ScheduledReductionFields.ReduceKey);

						var bucket = value.ReadInt(ScheduledReductionFields.Bucket);
						var rowKey = Tuple.Create(reduceKeyFromDb, bucket);
					    var thisIsNewScheduledReductionRow = deleter.Delete(iterator.CurrentKey, Etag.Parse(value.ReadBytes(ScheduledReductionFields.Etag)));
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

						if (getItemsToReduceParams.Take <= 0)
							yield break;
					}
					while (iterator.MoveNext());
				}

				getItemsToReduceParams.ReduceKeys.Remove(reduceKey);

				if (getItemsToReduceParams.Take <= 0)
                    yield break;
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
		    var reduceKeyHash = HashKey(reduceKey);
            var viewAndReduceKeyAndLevelAndBucket = (Slice)CreateKey(view, ReduceKeySizeLimited(reduceKey), reduceKeyHash, level, bucket);

			var reduceResultsByViewAndReduceKeyAndLevelAndBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
			var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);
			using (var iterator = reduceResultsByViewAndReduceKeyAndLevelAndBucket.MultiRead(Snapshot, viewAndReduceKeyAndLevelAndBucket))
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
					var value = LoadStruct(tableStorage.ReduceResults, iterator.CurrentKey, writeBatch.Value, out version);
					var size = tableStorage.ReduceResults.GetDataSize(Snapshot, iterator.CurrentKey);

					var readReduceKey = value.ReadString(ReduceResultFields.ReduceKey);

					yield return new MappedResultInfo
					{
						ReduceKey = readReduceKey,
						Etag = Etag.Parse(value.ReadBytes(ReduceResultFields.Etag)),
						Timestamp = DateTime.FromBinary(value.ReadLong(ReduceResultFields.Timestamp)),
						Bucket = value.ReadInt(ReduceResultFields.Bucket),
						Source = null,
						Size = size,
						Data = loadData ? LoadMappedResult(iterator.CurrentKey, readReduceKey, reduceResultsData) : null
					};
				}
				while (iterator.MoveNext());
			}
		}

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(int view, string reduceKey, int bucket, bool loadData)
		{
		    var reduceKeyHash = HashKey(reduceKey);
            var viewAndReduceKeyAndSourceBucket = (Slice)CreateKey(view, reduceKey, reduceKeyHash, bucket);

			var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

			using (var iterator = mappedResultsByViewAndReduceKeyAndSourceBucket.MultiRead(Snapshot, viewAndReduceKeyAndSourceBucket))
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
					var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
					var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);

					var readReduceKey = value.ReadString(MappedResultFields.ReduceKey);
					yield return new MappedResultInfo
					{
						ReduceKey = readReduceKey,
						Etag = Etag.Parse(value.ReadBytes(MappedResultFields.Etag)),
						Timestamp = DateTime.FromBinary(value.ReadLong(MappedResultFields.Timestamp)),
						Bucket = value.ReadInt(MappedResultFields.Bucket),
						Source = null,
						Size = size,
						Data = loadData ? LoadMappedResult(iterator.CurrentKey, readReduceKey, mappedResultsData) : null
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
			foreach (Etag etag in itemsToDelete)
			{
                var etagAsString = (Slice)etag.ToString();

				ushort version;
				var value = LoadStruct(tableStorage.ScheduledReductions, etagAsString, writeBatch.Value, out version);
				if (value == null)
					continue;

				if (etag.CompareTo(currentEtag) > 0)
				{
					hasResult = true;
					result.Etag = etag;
					result.Timestamp = DateTime.FromBinary(value.ReadLong(ScheduledReductionFields.Timestamp));
				}

				var view = value.ReadInt(ScheduledReductionFields.IndexId);
				var level = value.ReadInt(ScheduledReductionFields.Level);
				var reduceKey = value.ReadString(ScheduledReductionFields.ReduceKey);

				DeleteScheduledReduction(etagAsString, view, CreateKey(view), level, reduceKey);
			}

			return hasResult ? result : null;
		}

		public void DeleteScheduledReduction(int view, int level, string reduceKey)
		{
		    var reduceKeyHash = HashKey(reduceKey);
            var viewKey = CreateKey(view);

			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
            using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(Snapshot, (Slice)AppendToKey(viewKey, level, reduceKey, reduceKeyHash)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey;
					DeleteScheduledReduction(id, view, viewKey, level, reduceKey);
				}
				while (iterator.MoveNext());
			}
		}

		public void PutReducedResult(int view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
			var reduceResultsByViewAndReduceKeyAndLevelAndBucket =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
			var reduceResultsByViewAndReduceKeyAndLevel =
				tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
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
            var idAsSlice = (Slice)id.ToString();
		    var reduceKeyHash = HashKey(reduceKey);

			var reduceResult = new Structure<ReduceResultFields>(tableStorage.ReduceResults.Schema)
				.Set(ReduceResultFields.IndexId, view)
				.Set(ReduceResultFields.Etag, id.ToByteArray())
				.Set(ReduceResultFields.ReduceKey, reduceKey)
				.Set(ReduceResultFields.Level, level)
				.Set(ReduceResultFields.SourceBucket, sourceBucket)
				.Set(ReduceResultFields.Bucket, bucket)
				.Set(ReduceResultFields.Timestamp, SystemTime.UtcNow.ToBinary());

			tableStorage.ReduceResults.AddStruct(writeBatch.Value, idAsSlice, reduceResult, 0);

			ms.Position = 0;
			reduceResultsData.Add(writeBatch.Value, idAsSlice, ms, 0);

            var viewKey = CreateKey(view);
            var viewAndReduceKeyAndLevel = AppendToKey(viewKey, ReduceKeySizeLimited(reduceKey), reduceKeyHash, level);
            var viewAndReduceKeyAndLevelAndSourceBucket = AppendToKey(viewAndReduceKeyAndLevel, sourceBucket);
            var viewAndReduceKeyAndLevelAndBucket = AppendToKey(viewAndReduceKeyAndLevel, bucket);

            reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiAdd(writeBatch.Value, (Slice)viewAndReduceKeyAndLevelAndSourceBucket, idAsSlice);
            reduceResultsByViewAndReduceKeyAndLevel.MultiAdd(writeBatch.Value, (Slice)viewAndReduceKeyAndLevel, idAsSlice);
            reduceResultsByViewAndReduceKeyAndLevelAndBucket.MultiAdd(writeBatch.Value, (Slice)viewAndReduceKeyAndLevelAndBucket, idAsSlice);
            reduceResultsByView.MultiAdd(writeBatch.Value, (Slice)viewKey, idAsSlice);
		}

		public void RemoveReduceResults(int view, int level, string reduceKey, int sourceBucket)
		{
		    var reduceKeyHash = HashKey(reduceKey);
            var viewAndReduceKeyAndLevelAndSourceBucket = CreateKey(view, ReduceKeySizeLimited(reduceKey), reduceKeyHash, level, sourceBucket);
			var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);

			using (var iterator = reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiRead(Snapshot, (Slice)viewAndReduceKeyAndLevelAndSourceBucket))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

                RemoveReduceResult(iterator);
			}
		}

		public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(int view, int take, int limitOfItemsToReduceInSingleStep)
		{
			if (take <= 0)
				take = 1;

			var allKeysToReduce = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

			var key = (Slice) CreateKey(view);
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, key))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					yield break;

                var processedItems = 0;

				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);

					allKeysToReduce.Add(value.ReadString(ScheduledReductionFields.ReduceKey));
                    processedItems++;
				}
				while (iterator.MoveNext() && processedItems < take);
			}

            foreach (var reduceKey in allKeysToReduce)
            {
                var count = GetNumberOfMappedItemsPerReduceKey(view, reduceKey);
                var reduceType = count >= limitOfItemsToReduceInSingleStep ? ReduceType.MultiStep : ReduceType.SingleStep;
                yield return new ReduceTypePerKey(reduceKey, reduceType);
            }
		}

		private int GetNumberOfMappedItemsPerReduceKey(int view, string reduceKey)
		{
		    var reduceKeyHash = HashKey(reduceKey);
            var key = (Slice) CreateKey(view, reduceKey, reduceKeyHash);

			ushort version;
			var value = LoadStruct(tableStorage.ReduceKeyCounts, key, writeBatch.Value, out version);
			if (value == null)
				return 0;

			return value.ReadInt(ReduceKeyCountFields.MappedItemsCount);
		}

		public void UpdatePerformedReduceType(int view, string reduceKey, ReduceType reduceType)
		{
            var reduceKeyHash = HashKey(reduceKey);
            var key = (Slice)CreateKey(view, ReduceKeySizeLimited(reduceKey), reduceKeyHash);
			var version = tableStorage.ReduceKeyTypes.ReadVersion(Snapshot, key, writeBatch.Value);

			AddReduceKeyType(key, view, reduceKey, reduceType, version);
		}

        private void DeleteReduceKeyCount( Slice key, int view, Slice viewKey, ushort? expectedVersion)
		{
			var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);

			tableStorage.ReduceKeyCounts.Delete(writeBatch.Value, key, expectedVersion);
			reduceKeyCountsByView.MultiDelete(writeBatch.Value, viewKey, key);
		}

        private void DeleteReduceKeyType(Slice key, int view, Slice viewKey, ushort? expectedVersion)
		{
			var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

			tableStorage.ReduceKeyTypes.Delete(writeBatch.Value, key, expectedVersion);
			reduceKeyTypesByView.MultiDelete(writeBatch.Value, viewKey, key);
		}

        private void AddReduceKeyCount(Slice key, int view, Slice viewKey, string reduceKey, int count, ushort? expectedVersion)
		{
			var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);

			tableStorage.ReduceKeyCounts.AddStruct(
				writeBatch.Value,
				key,
				new Structure<ReduceKeyCountFields>(tableStorage.ReduceKeyCounts.Schema)
					.Set(ReduceKeyCountFields.IndexId, view)
					.Set(ReduceKeyCountFields.MappedItemsCount, count)
					.Set(ReduceKeyCountFields.ReduceKey, reduceKey), 
				expectedVersion);

            reduceKeyCountsByView.MultiAdd(writeBatch.Value, viewKey, key);
		}

		private void AddReduceKeyType(Slice key, int view, string reduceKey, ReduceType status, ushort? expectedVersion)
		{
			var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

			tableStorage.ReduceKeyTypes.AddStruct(
				writeBatch.Value,
				key,
				new Structure<ReduceKeyTypeFields>(tableStorage.ReduceKeyTypes.Schema)
					.Set(ReduceKeyTypeFields.IndexId, view)
					.Set(ReduceKeyTypeFields.ReduceType, (int) status)
					.Set(ReduceKeyTypeFields.ReduceKey, reduceKey),
				expectedVersion);

            reduceKeyTypesByView.MultiAdd(writeBatch.Value, (Slice)CreateKey(view), key);
		}

		public ReduceType GetLastPerformedReduceType(int view, string reduceKey)
		{
            var reduceKeyHash = HashKey(reduceKey);
            var key = (Slice)CreateKey(view, reduceKey, reduceKeyHash);

			ushort version;
			var value = LoadStruct(tableStorage.ReduceKeyTypes, key, writeBatch.Value, out version);
			if (value == null)
				return ReduceType.None;

			return (ReduceType)value.ReadInt(ReduceKeyTypeFields.ReduceType);
		}

		public IEnumerable<int> GetMappedBuckets(int view, string reduceKey)
		{
            var reduceKeyHash = HashKey(reduceKey);
            var viewAndReduceKey = (Slice) CreateKey(view, reduceKey, reduceKeyHash);
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);

			using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(Snapshot, viewAndReduceKey))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					yield break;

				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);

					yield return value.ReadInt(MappedResultFields.Bucket);
				}
				while (iterator.MoveNext());
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResults(int view, HashSet<string> keysLeftToReduce, bool loadData, int take, HashSet<string> keysReturned)
		{
			var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
			var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);
			var keysToReduce = new HashSet<string>(keysLeftToReduce);
			foreach (var reduceKey in keysToReduce)
			{
				keysLeftToReduce.Remove(reduceKey);
				
                var reduceKeyHash = HashKey(reduceKey);
                var viewAndReduceKey = (Slice)CreateKey(view, ReduceKeySizeLimited(reduceKey), reduceKeyHash);
				using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(Snapshot, viewAndReduceKey))
				{
					keysReturned.Add(reduceKey);

					if (!iterator.Seek(Slice.BeforeAllKeys))
						continue;

					do
					{
						ushort version;
						var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
						var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);

						var readReduceKey = value.ReadString(MappedResultFields.ReduceKey);

						yield return new MappedResultInfo
						{
							Bucket = value.ReadInt(MappedResultFields.Bucket),
							ReduceKey = readReduceKey,
							Etag = Etag.Parse(value.ReadBytes(MappedResultFields.Etag)),
							Timestamp = DateTime.FromBinary(value.ReadLong(MappedResultFields.Timestamp)),
							Data = loadData ? LoadMappedResult(iterator.CurrentKey, readReduceKey, mappedResultsData) : null,
							Size = size
						};
					}
					while (iterator.MoveNext());
				}

				if (take < 0)
				{
					yield break;
				}
			}
		}

		private RavenJObject LoadMappedResult(Slice key, string reduceKey, Index dataIndex)
		{
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
            using (var iterator = reduceKeyTypesByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
					yield break;

				var count = 0;
				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.ReduceKeyTypes, iterator.CurrentKey, writeBatch.Value, out version);

					yield return new ReduceTypePerKey(value.ReadString(ReduceKeyTypeFields.ReduceKey), (ReduceType) value.ReadInt(ReduceKeyTypeFields.ReduceType));

					count++;
				}
				while (iterator.MoveNext() && count < take);
			}
		}

		public void DeleteScheduledReductionForView(int view)
		{
			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);

            using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var id = iterator.CurrentKey.Clone();

					ushort version;
					var value = LoadStruct(tableStorage.ScheduledReductions, id, writeBatch.Value, out version);
					if (value == null)
						continue;

					var v = value.ReadInt(ScheduledReductionFields.IndexId);
					var level = value.ReadInt(ScheduledReductionFields.Level);
					var reduceKey = value.ReadString(ScheduledReductionFields.ReduceKey);

					DeleteScheduledReduction(id, v, CreateKey(v), level, reduceKey);
					storageActionsAccessor.General.MaybePulseTransaction();
				}
				while (iterator.MoveNext());
			}
		}



		public void RemoveReduceResultsForView(int view)
		{
			var reduceResultsByView = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);

            using (var iterator = reduceResultsByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

                RemoveReduceResult(iterator, PulseTransaction);
			}
		}

		private void DeleteScheduledReduction(Slice id, int view, string viewKey, int level, string reduceKey)
		{
		    var reduceKeyHash = HashKey(reduceKey);

			var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
			var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

			tableStorage.ScheduledReductions.Delete(writeBatch.Value, id);

            scheduledReductionsByView.MultiDelete(writeBatch.Value, (Slice)viewKey, id);
            scheduledReductionsByViewAndLevelAndReduceKey.MultiDelete(writeBatch.Value, (Slice)AppendToKey(viewKey, level, ReduceKeySizeLimited(reduceKey), reduceKeyHash), id);
		}

        private void RemoveReduceResult(global::Voron.Trees.IIterator iterator, Action afterRecordDeleted = null)
        {
            var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
            var reduceResultsByViewAndReduceKeyAndLevel = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
            var reduceResultsByViewAndReduceKeyAndLevelAndBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
            var reduceResultsByView = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);
            var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

            do
            {
                // TODO: Check if we can avoid the clone.
                Slice id = iterator.CurrentKey.Clone();

                ushort version;
                var value = LoadStruct(tableStorage.ReduceResults, id, writeBatch.Value, out version);

                var view = value.ReadInt(ReduceResultFields.IndexId);
                var reduceKey = value.ReadString(ReduceResultFields.ReduceKey);
                var level = value.ReadInt(ReduceResultFields.Level);
                var bucket = value.ReadInt(ReduceResultFields.Bucket);
                var sourceBucket = value.ReadInt(ReduceResultFields.SourceBucket);

                var reduceKeyHash = HashKey(reduceKey);

                var viewKey = CreateKey(view);
                var viewAndReduceKeyAndLevel = AppendToKey(viewKey, ReduceKeySizeLimited(reduceKey), reduceKeyHash, level);
                var viewAndReduceKeyAndLevelAndSourceBucket = AppendToKey(viewAndReduceKeyAndLevel, sourceBucket);
                var viewAndReduceKeyAndLevelAndBucket = AppendToKey(viewAndReduceKeyAndLevel, bucket);

                tableStorage.ReduceResults.Delete(writeBatch.Value, id);

                reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiDelete(writeBatch.Value, (Slice)viewAndReduceKeyAndLevelAndSourceBucket, id);
                reduceResultsByViewAndReduceKeyAndLevel.MultiDelete(writeBatch.Value, (Slice)viewAndReduceKeyAndLevel, id);
                reduceResultsByViewAndReduceKeyAndLevelAndBucket.MultiDelete(writeBatch.Value, (Slice)viewAndReduceKeyAndLevelAndBucket, id);
                reduceResultsByView.MultiDelete(writeBatch.Value, (Slice)viewKey, id);

                reduceResultsData.Delete(writeBatch.Value, id);

                if (afterRecordDeleted != null)
                    afterRecordDeleted();
            }
            while (iterator.MoveNext());
        }

        private void PulseTransaction()
        {
            storageActionsAccessor.General.MaybePulseTransaction();
        }

        private static string HashKey(string key)
        {
            return Convert.ToBase64String(Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(key)));
        }
	}

    internal class ScheduledReductionDeleter
    {
        private readonly ConcurrentSet<object> innerSet;

        private readonly IDictionary<Slice, object> state = new Dictionary<Slice, object>(new SliceComparer());

        public ScheduledReductionDeleter(ConcurrentSet<object> set, Func<object, Slice> extractKey)
        {
            innerSet = set;

            InitializeState(set, extractKey);
        }

        private void InitializeState(IEnumerable<object> set, Func<object, Slice> extractKey)
        {
            foreach (var item in set)
            {
                var key = extractKey(item);
                if (key == null)
                    continue;

                state.Add(key, null);
            }
        }

        public bool Delete(Slice key, object value)
        {
            if (state.ContainsKey(key))
                return false;

            state.Add(key, null);
            innerSet.Add(value);

            return true;
        }
    }
}