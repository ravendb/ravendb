//-----------------------------------------------------------------------
// <copyright file="MappedResults.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Json.Linq;
using System.Linq;
using Raven.Abstractions.Logging;

namespace Raven.Storage.Esent.StorageActions
{
	using Raven.Abstractions.Util.Encryptors;

	public partial class DocumentStorageActions : IMappedResultsStorageAction
	{
		private static readonly ThreadLocal<IHashEncryptor> localSha1 = new ThreadLocal<IHashEncryptor>(() => Encryptor.Current.CreateHash());

		public static byte[] HashReduceKey(string reduceKey)
		{
			return localSha1.Value.Compute20(Encoding.UTF8.GetBytes(reduceKey));
		}

		public void PutMappedResult(int indexId, string docId, string reduceKey, RavenJObject data)
		{
			Etag etag = uuidGenerator.CreateSequentialUuid(UuidType.MappedResults);
			using (var update = new Update(session, MappedResults, JET_prep.Insert))
			{
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], indexId);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"], docId, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["hashed_reduce_key"], HashReduceKey(reduceKey));
				var mapBucket = IndexingUtil.MapBucket(docId);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"], mapBucket);

				using (Stream stream = new BufferedStream(new ColumnStream(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"])))
				{
					using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
					{
						data.WriteTo(dataStream);
						dataStream.Flush();
					}
				}

				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"], etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"], SystemTime.UtcNow.ToBinary());

				update.Save();
			}
		}

		public IEnumerable<ReduceKeyAndCount> GetKeysStats(int view, int start, int pageSize)
		{
			Api.JetSetCurrentIndex(session, ReduceKeysCounts, "by_view_and_hashed_reduce_key");
			Api.MakeKey(session, ReduceKeysCounts, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, ReduceKeysCounts, SeekGrbit.SeekGE) == false)
				yield break;

			while (start > 0)
			{
				var viewFromDb = Api.RetrieveColumnAsInt32(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["view"]);
				if (view != viewFromDb)
					yield break;
				start--;
				if (Api.TryMoveNext(session, ReduceKeysCounts) == false)
					yield break;
			}

			do
			{
				var count =
					Api.RetrieveColumnAsInt32(session, ReduceKeysCounts,
											  tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"]).Value;
			    var viewFromDb = Api.RetrieveColumnAsInt32(session, ReduceKeysCounts,
			                                               tableColumnsCache.ReduceKeysCountsColumns["view"]);

				if (view != viewFromDb)
					continue;

				var key = Api.RetrieveColumnAsString(session, ReduceKeysCounts,
													 tableColumnsCache.ReduceKeysCountsColumns["reduce_key"], Encoding.Unicode);

				pageSize--;
				yield return new ReduceKeyAndCount
				{
					Count = count,
					Key = key
				};
			} while (Api.TryMoveNext(session, ReduceKeysCounts) && pageSize > 0);
		}


		public void PutReducedResult(int view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			Etag etag = uuidGenerator.CreateSequentialUuid(UuidType.ReduceResults);

			using (var update = new Update(session, ReducedResults, JET_prep.Insert))
			{
			    Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["view"], view);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["level"], level);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["hashed_reduce_key"], HashReduceKey(reduceKey));
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
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["timestamp"], SystemTime.UtcNow.ToBinary());

				update.Save();
			}
		}

		public void ScheduleReductions(int view, int level, ReduceKeyAndBucket reduceKeysAndBucket)
		{
			var bucket = reduceKeysAndBucket.Bucket;

			using (var map = new Update(session, ScheduledReductions, JET_prep.Insert))
			{
			    Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["view"], view);
				Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["reduce_key"],
							  reduceKeysAndBucket.ReduceKey, Encoding.Unicode);
				Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["hashed_reduce_key"],
							  HashReduceKey(reduceKeysAndBucket.ReduceKey));

					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["etag"], uuidGenerator.CreateSequentialUuid(UuidType.ScheduledReductions).ToByteArray());

				Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["timestamp"],
							  SystemTime.UtcNow.ToBinary());


				Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["bucket"],
							  bucket);

				Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["level"], level);
				map.Save();
			}
		}

		public ScheduledReductionInfo DeleteScheduledReduction(IEnumerable<object> itemsToDelete)
		{
			if (itemsToDelete == null)
				return null;
			
			var hasResult = false;
			var result = new ScheduledReductionInfo();
			
			var currentEtagBinary = Guid.Empty.ToByteArray();
			foreach (OptimizedDeleter reader in itemsToDelete.Where(x => x != null))
			{
				foreach (var sortedBookmark in reader.GetSortedBookmarks())
				{
					Api.JetGotoBookmark(session, ScheduledReductions, sortedBookmark.Item1, sortedBookmark.Item2);
					var etagBinary = Api.RetrieveColumn(session, ScheduledReductions,
					                                    tableColumnsCache.ScheduledReductionColumns["etag"]);
					if (new ComparableByteArray(etagBinary).CompareTo(currentEtagBinary) > 0)
					{
						hasResult = true;
						var timestamp =
							Api.RetrieveColumnAsInt64(session, ScheduledReductions,
							                          tableColumnsCache.ScheduledReductionColumns["timestamp"]).Value;
						result.Etag = Etag.Parse(etagBinary);
						result.Timestamp = DateTime.FromBinary(timestamp);
					}

					Api.JetDelete(session, ScheduledReductions);
				}
			}
			return hasResult ? result : null;
		}

		public void DeleteScheduledReduction(int view, int level, string reduceKey)
		{
			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view_level_and_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, ScheduledReductions, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ScheduledReductions, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ScheduledReductions, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			Api.MakeKey(session, ScheduledReductions, 0, MakeKeyGrbit.None);
			if (Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekGE) == false)
				return;

			Api.MakeKey(session, ScheduledReductions, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ScheduledReductions, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ScheduledReductions, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			Api.MakeKey(session, ScheduledReductions, int.MaxValue, MakeKeyGrbit.None);
			if(Api.TrySetIndexRange(session, ScheduledReductions, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit) == false)
				return;

			do
			{
				var indexFromDb = Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["view"], RetrieveColumnGrbit.RetrieveFromIndex);
				var levelFromDb =
							Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["level"], RetrieveColumnGrbit.RetrieveFromIndex).
								Value;
				var reduceKeyFromDb = Api.RetrieveColumnAsString(session, ScheduledReductions,
											   tableColumnsCache.ScheduledReductionColumns["reduce_key"]);

				if (view != indexFromDb)
					continue;
				if (levelFromDb != level)
					continue;
				if (string.Equals(reduceKeyFromDb, reduceKey, StringComparison.Ordinal) == false)
					continue;

				Api.JetDelete(Session, ScheduledReductions);
			} while (Api.TryMoveNext(Session, ScheduledReductions));
		}


		public IEnumerable<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams)
		{
			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view_level_and_hashed_reduce_key_and_bucket");
			var seenLocally = new HashSet<Tuple<string, int>>();
			foreach (var reduceKey in getItemsToReduceParams.ReduceKeys.ToArray())
			{
				Api.MakeKey(session, ScheduledReductions, getItemsToReduceParams.Index, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, ScheduledReductions, getItemsToReduceParams.Level, MakeKeyGrbit.None);
				Api.MakeKey(session, ScheduledReductions, HashReduceKey(reduceKey), MakeKeyGrbit.None);
				Api.MakeKey(session, ScheduledReductions, 0, MakeKeyGrbit.None);
				if (Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekGE) == false)
					continue;

				Api.MakeKey(session, ScheduledReductions, getItemsToReduceParams.Index, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, ScheduledReductions, getItemsToReduceParams.Level, MakeKeyGrbit.None);
				Api.MakeKey(session, ScheduledReductions, HashReduceKey(reduceKey), MakeKeyGrbit.None);
				Api.MakeKey(session, ScheduledReductions, int.MaxValue, MakeKeyGrbit.None);

				if(Api.TrySetIndexRange(session, ScheduledReductions, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit) == false)
					continue;

				// this isn't used for optimized reading, but to make it easier to delete records later on
				OptimizedDeleter reader;
				if (getItemsToReduceParams.ItemsToDelete.Count == 0)
				{
					getItemsToReduceParams.ItemsToDelete.Add(reader = new OptimizedDeleter());
				}
				else
				{
					reader = (OptimizedDeleter)getItemsToReduceParams.ItemsToDelete.First();
				}
				do
				{
                    if (getItemsToReduceParams.Take <= 0)
                        break;
					var indexFromDb = Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["view"], RetrieveColumnGrbit.RetrieveFromIndex);
					var levelFromDb =
						Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["level"], RetrieveColumnGrbit.RetrieveFromIndex).
							Value;
					var reduceKeyFromDb = Api.RetrieveColumnAsString(session, ScheduledReductions,
												   tableColumnsCache.ScheduledReductionColumns["reduce_key"]);

					if (getItemsToReduceParams.Index != indexFromDb)
						continue;
					if (levelFromDb != getItemsToReduceParams.Level)
						continue;
					if (string.Equals(reduceKeyFromDb, reduceKey, StringComparison.Ordinal) == false)
						continue;

					var bucket =
							Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["bucket"]).Value;

					var rowKey = Tuple.Create(reduceKeyFromDb, bucket);
					var thisIsNewScheduledReductionRow = reader.Add(session, ScheduledReductions);
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
				} while (Api.TryMoveNext(session, ScheduledReductions));

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

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(int view, string reduceKey, int bucket, bool loadData)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			Api.MakeKey(session, MappedResults, bucket, MakeKeyGrbit.None);

			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
			{
				yield return new MappedResultInfo
				{
					ReduceKey = reduceKey,
					Bucket = bucket
				};
				yield break;
			}

			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			Api.MakeKey(session, MappedResults, bucket, MakeKeyGrbit.None);

			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			bool returnedResults = false;
			do
			{
				var indexFromDb = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
				var bucketFromDb = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
				if (indexFromDb != view ||
					bucketFromDb != bucket ||
					string.Equals(keyFromDb, reduceKey, StringComparison.Ordinal) == false // the key is explicitly compared using case sensitive approach
					)
				{
					// we might have a hash collision, so we will just skip and try the next one
					continue;
				}
				var timestamp = Api.RetrieveColumnAsInt64(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).Value;
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey =
						keyFromDb,
					Etag = Etag.Parse(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
					Timestamp = DateTime.FromBinary(timestamp),
					Data = loadData ? LoadMappedResults(keyFromDb) : null,
					Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0
				};
				returnedResults = true;
			} while (Api.TryMoveNext(session, MappedResults));

			if (returnedResults == false)
			{
				{
					yield return new MappedResultInfo
					{
						ReduceKey = reduceKey,
						Bucket = bucket
					};
				}
			}
		}


		public void RemoveReduceResults(int view, int level, string reduceKey, int sourceBucket)
		{
			Api.JetSetCurrentIndex(session, ReducedResults, "by_view_level_source_bucket_and_hashed_reduce_key");
			Api.MakeKey(session, ReducedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, sourceBucket, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			if (Api.TrySeek(session, ReducedResults, SeekGrbit.SeekEQ) == false)
				return;

			do
			{
				var indexFromDb = Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["view"], RetrieveColumnGrbit.RetrieveFromIndex);
				var bucketFromDb = Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["source_bucket"], RetrieveColumnGrbit.RetrieveFromIndex).Value;
				if (indexFromDb != view ||
					bucketFromDb != sourceBucket)
				{
					break;
				}

				var keyFromDb = Api.RetrieveColumnAsString(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["reduce_key"]);
				if (string.Equals(keyFromDb, reduceKey, StringComparison.Ordinal) == false)// case sensitive check on purpose
					continue;


				Api.JetDelete(session, ReducedResults);
			} while (Api.TryMoveNext(session, ReducedResults));
		}

		private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(int view, string reduceKey, int level, int bucket, bool loadData)
		{
			Api.JetSetCurrentIndex(session, ReducedResults, "by_view_level_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, ReducedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, bucket, MakeKeyGrbit.None);

			if (Api.TrySeek(session, ReducedResults, SeekGrbit.SeekEQ) == false)
			{
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = reduceKey,
				};
				yield break;
			}

			Api.MakeKey(session, ReducedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, bucket, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, ReducedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
			bool returnedResults = false;
			do
			{
				var key = Api.RetrieveColumnAsString(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["reduce_key"]);
				var bucketFromDb = Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["bucket"]).Value;
				if (bucketFromDb != bucket)
					break;

				// we explicitly compare the key just as we would during the group by phase, using case sensitive approach
				if (string.Equals(key, reduceKey, StringComparison.Ordinal) == false)
					continue;
				returnedResults = true;
				var timestamp = Api.RetrieveColumnAsInt64(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["timestamp"]).Value;
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey =
						key,
					Etag = Etag.Parse(Api.RetrieveColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["etag"])),
					Timestamp = DateTime.FromBinary(timestamp),
					Data = loadData ? LoadReducedResults(key) : null,
					Size = Api.RetrieveColumnSize(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"]) ?? 0
				};
			} while (Api.TryMoveNext(session, ReducedResults));

			if (returnedResults == false)
			{
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = reduceKey,
				};
			}

		}
		public void DeleteMappedResultsForDocumentId(string documentId, int view, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_doc_key");
			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
				return;

			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				var documentIdFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"]);
				if (StringComparer.OrdinalIgnoreCase.Equals(documentIdFromDb, documentId) == false)
					continue;
				var reduceKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"],
														   Encoding.Unicode);
				var bucket = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;

				var key = new ReduceKeyAndBucket(bucket, reduceKey);
				removed[key] = removed.GetOrDefault(key) + 1;
				Api.JetDelete(session, MappedResults);
			} while (Api.TryMoveNext(session, MappedResults));
		}

		public void UpdateRemovedMapReduceStats(int indexId, Dictionary<ReduceKeyAndBucket, int> removed)
		{
			foreach (var keyAndBucket in removed)
			{
				DecrementReduceKeyCounter(indexId, keyAndBucket.Key.ReduceKey, keyAndBucket.Value);
			}
		}

		public void DeleteMappedResultsForView(int indexId)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_doc_key");
			Api.MakeKey(session, MappedResults, indexId, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
				return;

			var deletedReduceKeys = new List<string>();

			do
			{
				var indexFromDb = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				if (indexId != indexFromDb)
					break;

				var reduceKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
				deletedReduceKeys.Add(reduceKey);

				Api.JetDelete(session, MappedResults);

			} while (Api.TryMoveNext(session, MappedResults));

			foreach (var reduceKey in deletedReduceKeys)
			{
				DecrementReduceKeyCounter(indexId, reduceKey, 1);
			}
		}

		public IEnumerable<string> GetKeysForIndexForDebug(int view, int start, int take)
		{
			if (take <= 0)
				yield break;

			Api.JetSetCurrentIndex(session, MappedResults, "by_view_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
				yield break;

			try
			{
				Api.JetMove(session, MappedResults, start, MoveGrbit.MoveKeyNE);
			}
			catch (EsentErrorException e)
			{
				if (e.Error == JET_err.NoCurrentRecord)
				{
					yield break;
				}
				throw;
			}

			var results = new HashSet<string>();
			do
			{
				var indexNameFromDb = Api.RetrieveColumnAsInt32(session, MappedResults,
																 tableColumnsCache.MappedResultsColumns["view"],
																 RetrieveColumnGrbit.RetrieveFromIndex);
				var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults,
														   tableColumnsCache.MappedResultsColumns["reduce_key"]);
			    var comparison = view - indexNameFromDb;
				if (comparison < 0)
					continue; // skip to the next item
				if (comparison > 0) // after the current item
					break;

				if (results.Add(keyFromDb))
				{
					take -= 1;
					yield return keyFromDb;
				}
			} while (Api.TryMoveNext(session, MappedResults) && take > 0);
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(int view, string key, int start, int take)
		{
			if (take <= 0)
				yield break;


			// NOTE, this intentionally does a table scan for all the items in the same index.
			// the reason it is allowed is that this is only applicable for debug, and never is used in production systems
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, HashReduceKey(key), MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
				yield break;
			if (TryMoveTableRecords(MappedResults, start, false))
				yield break;
			do
			{

				var indexNameFromDb = Api.RetrieveColumnAsInt32(session, MappedResults,
																 tableColumnsCache.MappedResultsColumns["view"], 
																 RetrieveColumnGrbit.RetrieveFromIndex);
				var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults,
														   tableColumnsCache.MappedResultsColumns["reduce_key"]);

			    var indexCompare = view - indexNameFromDb;

				if (indexCompare < 0)
					continue;
				if (indexCompare > 0)
					break;
				var keyCompare = string.Compare(key, keyFromDb, StringComparison.OrdinalIgnoreCase);
				if (keyCompare != 0)
					continue;

				take -= 1;

				var bucket =
					Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
				var timestamp = Api.RetrieveColumnAsInt64(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).Value;
				yield return new MappedResultInfo
				{
					ReduceKey = keyFromDb,
					Etag = Etag.Parse(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
					Timestamp = DateTime.FromBinary(timestamp),
					Data = LoadMappedResults(keyFromDb),
					Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0,
					Bucket = bucket,
					Source =
						Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"],
												   Encoding.Unicode)
				};

			} while (Api.TryMoveNext(session, MappedResults) && take > 0);
		}

		public IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(int view, int start, int take)
		{
			if (take <= 0)
				yield break;

			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view_level_and_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, ScheduledReductions, view, MakeKeyGrbit.NewKey);

			if (Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekGE) == false)
				yield break;

			if (TryMoveTableRecords(ScheduledReductions, start, false))
				yield break;

			do
			{
				var indexNameFromDb = Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["view"], 
																 RetrieveColumnGrbit.RetrieveFromIndex);

			    var indexCompare = view - indexNameFromDb;

				if (indexCompare < 0)
					continue;
				if (indexCompare > 0)
					break;

				var levelFromDb = Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["level"]).Value;

				var keyFromDb = Api.RetrieveColumnAsString(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["reduce_key"]);

				var etagFromDb = new Guid(Api.RetrieveColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["etag"]));

				var timestampFromDb = Api.RetrieveColumnAsInt64(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["timestamp"]).Value;
				var bucketFromDb = Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["bucket"]).Value;

				take--;

				yield return new ScheduledReductionDebugInfo
				{
					Key = keyFromDb,
					Level = levelFromDb,
					Etag = etagFromDb,
					Timestamp = DateTime.FromBinary(timestampFromDb),
					Bucket = bucketFromDb
				};

			} while (Api.TryMoveNext(session, ScheduledReductions) && take > 0);

		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(int view, string reduceKey, int level, int start, int take)
		{
			if (take <= 0)
				yield break;

			// NOTE, this intentionally does a table scan for all the items in the same index.
			// the reason it is allowed is that this is only applicable for debug, and never is used in production systems

			Api.JetSetCurrentIndex(session, ReducedResults, "by_view_level_source_bucket_and_hashed_reduce_key");
			Api.MakeKey(session, ReducedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			if (Api.TrySeek(session, ReducedResults, SeekGrbit.SeekGE) == false)
				yield break;

			if (TryMoveTableRecords(ReducedResults, start, false))
				yield break;
			do
			{

				var levelFromDb =
					Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["level"]).Value;
				var indexNameFromDb = Api.RetrieveColumnAsInt32(session, ReducedResults,
																 tableColumnsCache.ReduceResultsColumns["view"], 
																 RetrieveColumnGrbit.RetrieveFromIndex);
				var keyFromDb = Api.RetrieveColumnAsString(session, ReducedResults,
														   tableColumnsCache.ReduceResultsColumns["reduce_key"]);
			    var indexCompare = view - indexNameFromDb;

				if (indexCompare < 0)
					continue;
				if (indexCompare > 0)
					break;
				if (levelFromDb < level)
					continue;
				if (levelFromDb > level)
					break;
				var keyCompare = string.Compare(reduceKey, keyFromDb, StringComparison.OrdinalIgnoreCase);
				if (keyCompare != 0)
					continue;

				take -= 1;


				var timestamp = Api.RetrieveColumnAsInt64(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["timestamp"]).Value;
				yield return new MappedResultInfo
				{
					ReduceKey = keyFromDb,
					Etag = Etag.Parse(Api.RetrieveColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["etag"])),
					Timestamp = DateTime.FromBinary(timestamp),
					Data = LoadReducedResults(keyFromDb),
					Size = Api.RetrieveColumnSize(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"]) ?? 0,
					Bucket = Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["bucket"]).Value,
					Source =
						Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["source_bucket"]).
							ToString()
				};
			} while (Api.TryMoveNext(session, ReducedResults) && take > 0);
		}

		public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(int view, int take, int limitOfItemsToReduceInSingleStep)
		{
			var allKeysToReduce = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view_level_and_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, ScheduledReductions, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekGE) == false)
				yield break;

			var processedItems = 0;

			do
			{
				var indexFromDb = Api.RetrieveColumnAsInt32(session, ScheduledReductions,
															 tableColumnsCache.ScheduledReductionColumns["view"], 
															 RetrieveColumnGrbit.RetrieveFromIndex);

				if (view != indexFromDb)
					break;

				var reduceKey = Api.RetrieveColumnAsString(session, ScheduledReductions,
											   tableColumnsCache.ScheduledReductionColumns["reduce_key"]);

				allKeysToReduce.Add(reduceKey);
				processedItems++;

			} while (Api.TryMoveNext(session, ScheduledReductions) && processedItems < take);

			foreach (var reduceKey in allKeysToReduce)
			{
				var count = GetNumberOfMappedItemsPerReduceKey(view, reduceKey);
				var reduceType = count >= limitOfItemsToReduceInSingleStep ? ReduceType.MultiStep : ReduceType.SingleStep;
				yield return new ReduceTypePerKey(reduceKey, reduceType);
			}
		}

		public void UpdatePerformedReduceType(int view, string reduceKey, ReduceType performedReduceType)
		{
			ExecuteOnReduceKey(view, reduceKey, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns, () =>
			{
				using (var update = new Update(session, ReduceKeysStatus, JET_prep.Replace))
				{
					Api.SetColumn(session, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns["reduce_type"],
								  (int)performedReduceType);

					update.Save();
				}
			}, () => Api.SetColumn(session, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns["reduce_type"],
								  (int)performedReduceType));
		}

		private void ExecuteOnReduceKey(int view, string reduceKey,
			Table table,
			IDictionary<string, JET_COLUMNID> columnids,
			Action updateAction,
			Action insertAction)
		{
			var hashReduceKey = HashReduceKey(reduceKey);

			Api.JetSetCurrentIndex(session, table, "by_view_and_hashed_reduce_key");
			Api.MakeKey(session, table, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, table, hashReduceKey, MakeKeyGrbit.None);
			Api.MakeKey(session, table, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);

			if (Api.TrySeek(session, table, SeekGrbit.SeekEQ) == false)
			{
				if (insertAction == null)
					return;
				using (var update = new Update(session, table, JET_prep.Insert))
				{
				    Api.SetColumn(session, table, columnids["view"], view);
					Api.SetColumn(session, table, columnids["reduce_key"], reduceKey, Encoding.Unicode);
					Api.SetColumn(session, table, columnids["hashed_reduce_key"], hashReduceKey);

					insertAction();
					update.SaveAndGotoBookmark();
				}
				return;
			}

			Api.MakeKey(session, table, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, table, hashReduceKey, MakeKeyGrbit.None);
			Api.MakeKey(session, table, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);

			Api.TrySetIndexRange(session, table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			do
			{
				var reduceKeyFromDb = Api.RetrieveColumnAsString(session, table, columnids["reduce_key"]);
				if (StringComparer.Ordinal.Equals(reduceKey, reduceKeyFromDb) == false)
					continue;

				updateAction();
				return;
			} while (Api.TryMoveNext(session, table));

			// couldn't find it...

			if (insertAction == null)
				return;

			using (var update = new Update(session, table, JET_prep.Insert))
			{
			    Api.SetColumn(session, table, columnids["view"], view);
				Api.SetColumn(session, table, columnids["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, table, columnids["hashed_reduce_key"], hashReduceKey);

				insertAction();

				update.SaveAndGotoBookmark();
			}
		}

		public ReduceType GetLastPerformedReduceType(int view, string reduceKey)
		{
			int reduceType = 0;
			ExecuteOnReduceKey(view, reduceKey, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns, () =>
			{
				reduceType = Api.RetrieveColumnAsInt32(session, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns["reduce_type"]).Value;
			}, null);
			return (ReduceType)reduceType;
		}

		public IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(int view, int start, int take)
		{
			Api.JetSetCurrentIndex(session, ReduceKeysStatus, "by_view_and_hashed_reduce_key");
			Api.MakeKey(session, ReduceKeysStatus, view, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, ReduceKeysStatus, SeekGrbit.SeekGE) == false)
				yield break;


			if (TryMoveTableRecords(ReduceKeysStatus, start, false))
				yield break;

			do
			{

			    var indexFromDb = Api.RetrieveColumnAsInt32(session, ReduceKeysStatus,
			                                                tableColumnsCache.ReduceKeysStatusColumns["view"],
			                                                RetrieveColumnGrbit.RetrieveFromIndex);

                if (view != indexFromDb)
                    break; 

                var reduceKey = Api.RetrieveColumnAsString(session, ReduceKeysStatus,
											   tableColumnsCache.ReduceKeysStatusColumns["reduce_key"]);

				var reduceType = Api.RetrieveColumnAsInt32(session, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns["reduce_type"]).Value;

				take--;
				yield return new ReduceTypePerKey(reduceKey, (ReduceType) reduceType);

			} while (Api.TryMoveNext(session, ReduceKeysStatus) && take > 0);
		}

		public IEnumerable<int> GetMappedBuckets(int view, string reduceKey)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_hashed_reduce_key_and_bucket");
			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			Api.MakeKey(session, MappedResults, 0, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
				yield break;

			Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			Api.MakeKey(session, MappedResults, int.MaxValue, MakeKeyGrbit.None);
			if (Api.TrySetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive) == false)
				yield break;
			do
			{
				var viewFromDb = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				if (viewFromDb != view)
					continue;

				var rKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"],
														   Encoding.Unicode);

				if (StringComparer.OrdinalIgnoreCase.Equals(rKey, reduceKey) == false)
					continue;

				yield return Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
			} while (Api.TryMoveNext(session, MappedResults));
		}

		public IEnumerable<MappedResultInfo> GetMappedResults(int view, IEnumerable<string> keysToReduce, bool loadData)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_hashed_reduce_key_and_bucket");

			foreach (var reduceKey in keysToReduce)
			{
				Api.MakeKey(session, MappedResults, view, MakeKeyGrbit.NewKey);
				var hashReduceKey = HashReduceKey(reduceKey);
				Api.MakeKey(session, MappedResults, hashReduceKey, MakeKeyGrbit.None);
				if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
					continue;

				do
				{
					var indexFromDb = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
					var hashKeyFromDb = Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["hashed_reduce_key"]);

					if (indexFromDb != view ||
						hashReduceKey.SequenceEqual(hashKeyFromDb) == false)
					{
						break;
					}
					var timestamp = Api.RetrieveColumnAsInt64(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).Value;
					var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
					yield return new MappedResultInfo
					{
						Bucket = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value,
						ReduceKey = keyFromDb,
						Etag = Etag.Parse(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
						Timestamp = DateTime.FromBinary(timestamp),
						Data = loadData ? LoadMappedResults(keyFromDb) : null,
						Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0
					};
				} while (Api.TryMoveNext(session, MappedResults));
			}
		}

		private RavenJObject LoadMappedResults(string key)
		{
			using (Stream stream = new BufferedStream(new ColumnStream(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"])))
			using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Decode(key, null, ds)))
			{
				return dataStream.ToJObject();
			}
		}

		private RavenJObject LoadReducedResults(string key)
		{
			using (Stream stream = new BufferedStream(new ColumnStream(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"])))
			using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Decode(key, null, ds)))
			{
				return dataStream.ToJObject();
			}
		}

		public void IncrementReduceKeyCounter(int indexId, string reduceKey, int val)
		{
			try
			{
				ExecuteOnReduceKey(indexId, reduceKey, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns,
								   () => Api.EscrowUpdate(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"], val),
								   () => Api.SetColumn(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"], val));
			}
			catch (EsentErrorException e)
			{
				// we should NOT be getting this error, we still got it, and while I think I fixed the reason for that...
				// if we do, it is okay to ignore it in this specific instance, since it will just skew the number for reduce counts a bit
				// and it will all fix itself one way or the other
				if (e.Error != JET_err.WriteConflict)
					throw;
				logger.WarnException(
					"Could not update the reduce key counter for index " + indexId + ", key: " + reduceKey +
					". Ignoring this, multi step reduce promotion may be delayed for this value.", e);
			}
		}

		private void DecrementReduceKeyCounter(int view, string reduceKey, int value)
		{
			var removeReducedKeyStatus = false;

			ExecuteOnReduceKey(view, reduceKey, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns,
				() =>
				{
					var result = Api.EscrowUpdate(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"], -value);
					if (result == value)
					{
						Api.JetDelete(session, ReduceKeysCounts);
						removeReducedKeyStatus = true;
					}
				}, null);

			if (removeReducedKeyStatus)
			{
				ExecuteOnReduceKey(view, reduceKey, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns,
								   () => Api.JetDelete(session, ReduceKeysStatus), null);
			}
		}

		private int GetNumberOfMappedItemsPerReduceKey(int view, string reduceKey)
		{
			int numberOfMappedItemsPerReduceKey = 0;
			ExecuteOnReduceKey(view, reduceKey, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns, () =>
			{
				numberOfMappedItemsPerReduceKey = Api.RetrieveColumnAsInt32(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"]).Value;
			}, null);

			return numberOfMappedItemsPerReduceKey;
		}
	}
}
