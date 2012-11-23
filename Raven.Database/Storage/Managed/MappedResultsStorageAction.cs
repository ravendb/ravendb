//-----------------------------------------------------------------------
// <copyright file="MappedResultsStorageAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;
using System.Linq;
using Table = Raven.Munin.Table;

namespace Raven.Storage.Managed
{
	public class MappedResultsStorageAction : IMappedResultsStorageAction
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;
		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;

		public MappedResultsStorageAction(TableStorage storage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
		{
			this.storage = storage;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
		}

		public void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data)
		{
			var ms = new MemoryStream();

			using (var stream = documentCodecs.Aggregate((Stream)ms, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
			{
				data.WriteTo(stream);
			}
			var byteArray = generator.CreateSequentialUuid().ToByteArray();
			var key = new RavenJObject
			{
				{"view", view},
				{"reduceKey", reduceKey},
				{"docId", docId},
				{"etag", byteArray},
				{"bucket", IndexingUtil.MapBucket(docId)},
				{"timestamp", SystemTime.UtcNow}
			};
			storage.MappedResults.Put(key, ms.ToArray());
		}

		private RavenJObject LoadMappedResult(Table.ReadResult readResult)
		{
			var key = readResult.Key.Value<string>("reduceKey");

			Stream memoryStream = new MemoryStream(readResult.Data());
			using (var stream = documentCodecs.Aggregate(memoryStream, (ds, codec) => codec.Decode(key, null, ds)))
			{
				return stream.ToJObject();
			}
		}

		public void DeleteMappedResultsForDocumentId(string documentId, string view, HashSet<ReduceKeyAndBucket> removed)
		{
			foreach (var key in storage.MappedResults["ByViewAndDocumentId"].SkipTo(new RavenJObject
			{
				{"view", view},
				{"docId", documentId}
			}).TakeWhile(x => StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("view"), view) &&
							  StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("docId"), documentId)))
			{
				storage.MappedResults.Remove(key);
				removed.Add(new ReduceKeyAndBucket(key.Value<int>("bucket"), key.Value<string>("reduceKey")));
			}
		}

		public void DeleteMappedResultsForView(string view)
		{
			foreach (var key in storage.MappedResults["ByViewAndReduceKey"].SkipTo(new RavenJObject { { "view", view } })
			.TakeWhile(x => StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("view"), view)))
			{
				storage.MappedResults.Remove(key);
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsReduceKeysAfter(string indexName, Guid lastReducedEtag, bool loadData, int take)
		{
			var mappedResultInfos = storage.MappedResults["ByViewAndEtagDesc"]
				// the index is sorted view ascending and then etag descending
				// we index before this index, then backward toward the last one.
				.SkipBefore(new RavenJObject { { "view", indexName }, { "etag", lastReducedEtag.ToByteArray() } })
				.TakeWhile(x => StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("view"), indexName))
				.Select(key =>
				{

					var mappedResultInfo = new MappedResultInfo
					{
						ReduceKey = key.Value<string>("reduceKey"),
						Etag = new Guid(key.Value<byte[]>("etag")),
						Timestamp = key.Value<DateTime>("timestamp")
					};

					var readResult = storage.MappedResults.Read(key);
					if (readResult != null)
					{
						mappedResultInfo.Size = readResult.Size;
						if (loadData)
							mappedResultInfo.Data = LoadMappedResult(readResult);
					}

					return mappedResultInfo;
				});

			var results = new Dictionary<string, MappedResultInfo>();

			foreach (var mappedResultInfo in mappedResultInfos)
			{
				results[mappedResultInfo.ReduceKey] = mappedResultInfo;
				if (results.Count == take)
					break;
			}

			return results.Values;
		}

		public void ScheduleReductions(string view, int level, IEnumerable<ReduceKeyAndBucket> reduceKeysAndBuckets)
		{
			foreach (var reduceKeysAndBukcet in reduceKeysAndBuckets)
			{
				var etag = generator.CreateSequentialUuid().ToByteArray();
				storage.ScheduleReductions.UpdateKey(new RavenJObject
					{
						{"view", view},
						{"reduceKey", reduceKeysAndBukcet.ReduceKey},
						{"bucket", reduceKeysAndBukcet.Bucket},
						{"level", level},
						{"etag", etag},
						{"timestamp", SystemTime.UtcNow}
					});
			}
		}

		public ScheduledReductionInfo DeleteScheduledReduction(List<object> itemsToDelete)
		{
			var result = new ScheduledReductionInfo();
			var hasResult = false;
			var currentEtagBinary = Guid.Empty.ToByteArray();
			foreach (RavenJToken token in itemsToDelete)
			{
				var readResult = storage.ScheduleReductions.Read(token);
				if (readResult == null)
					continue;

				var etagBinary = readResult.Key.Value<byte[]>("etag");
				if (new ComparableByteArray(etagBinary).CompareTo(currentEtagBinary) > 0)
				{
					hasResult = true;
					var timestamp = readResult.Key.Value<DateTime>("timestamp");
					result.Etag = etagBinary.TransfromToGuidWithProperSorting();
					result.Timestamp = timestamp;
				}

				storage.ScheduleReductions.Remove(token);
			}
			return hasResult ? result : null;
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(string index, int level, int take, List<object> itemsToDelete)
		{
			var seen = new HashSet<Tuple<string, int>>();

			foreach (var result in storage.ScheduleReductions["ByViewLevelReduceKeyAndBucket"].SkipTo(new RavenJObject
			{
				{"view", index},
				{"level", level},
			}))
			{
				var indexFromDb = result.Value<string>("view");
				var levelFromDb = result.Value<int>("level");

				if (string.Equals(indexFromDb, index, StringComparison.InvariantCultureIgnoreCase) == false ||
					levelFromDb != level)
					break;

				var reduceKey = result.Value<string>("reduceKey");
				var bucket = result.Value<int>("bucket");

				if (seen.Add(Tuple.Create(reduceKey, bucket)))
				{
					foreach (var mappedResultInfo in GetResultsForBucket(index, level, reduceKey, bucket))
					{
						take--;
						yield return mappedResultInfo;
					}
				}
				itemsToDelete.Add(result);
				if (take <= 0)
					break;
			}
		}

		private IEnumerable<MappedResultInfo> GetResultsForBucket(string index, int level, string reduceKey, int bucket)
		{
			switch (level)
			{
				case 0:
					return GetMappedResultsForBucket(index, reduceKey, bucket);
				case 1:
				case 2:
					return GetReducedResultsForBucket(index, reduceKey, level, bucket);
				default:
					throw new ArgumentException("Invalid level: " + level);
			}
		}

		private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(string index, string reduceKey, int level, int bucket)
		{
			var results = storage.ReduceResults["ByViewReduceKeyLevelAndBucket"]
				.SkipTo(new RavenJObject
				{
					{"view", index},
					{"reduceKey", reduceKey},
					{"level", level},
					{"bucket", bucket}
				})
				.TakeWhile(x => string.Equals(index, x.Value<string>("view"), StringComparison.InvariantCultureIgnoreCase) &&
								string.Equals(reduceKey, x.Value<string>("reduceKey"), StringComparison.InvariantCultureIgnoreCase) &&
								level == x.Value<int>("level") &&
								bucket == x.Value<int>("bucket"));

			bool hasResults = false;
			foreach (var result in results)
			{
				hasResults = true;
				var readResult = storage.ReduceResults.Read(result);

				var mappedResultInfo = new MappedResultInfo
				{
					ReduceKey = readResult.Key.Value<string>("reduceKey"),
					Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
					Timestamp = readResult.Key.Value<DateTime>("timestamp"),
					Bucket = readResult.Key.Value<int>("bucket"),
					Source = readResult.Key.Value<int>("sourceBucket").ToString(),
					Size = readResult.Size,
					Data = LoadMappedResult(readResult)
				};

				yield return mappedResultInfo;
			}

			if (hasResults)
				yield break;

			yield return new MappedResultInfo
			{
				Bucket = bucket,
				ReduceKey = reduceKey
			};
		}

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(string index, string reduceKey, int bucket)
		{
			var results = storage.MappedResults["ByViewReduceKeyAndBucket"]
				.SkipTo(new RavenJObject
				{
					{"view", index},
					{"reduceKey", reduceKey},
					{"bucket", bucket}
				})
				.TakeWhile(x => string.Equals(index, x.Value<string>("view"), StringComparison.InvariantCultureIgnoreCase) &&
								string.Equals(reduceKey, x.Value<string>("reduceKey"), StringComparison.InvariantCultureIgnoreCase) &&
								bucket == x.Value<int>("bucket"));

			bool hasResults = false;
			foreach (var result in results)
			{
				hasResults = true;
				var readResult = storage.MappedResults.Read(result);

				yield return new MappedResultInfo
				{
					ReduceKey = readResult.Key.Value<string>("reduceKey"),
					Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
					Timestamp = readResult.Key.Value<DateTime>("timestamp"),
					Bucket = readResult.Key.Value<int>("bucket"),
					Source = readResult.Key.Value<string>("docId"),
					Size = readResult.Size,
					Data = LoadMappedResult(readResult)
				};
			}

			if (hasResults)
				yield break;

			yield return new MappedResultInfo
			{
				Bucket = bucket,
				ReduceKey = reduceKey
			};
		}

		public void PutReducedResult(string name, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			var ms = new MemoryStream();

			using (var stream = documentCodecs.Aggregate((Stream)ms, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
			{
				data.WriteTo(stream);
			}

			var etag = generator.CreateSequentialUuid().ToByteArray();

			storage.ReduceResults.Put(new RavenJObject
			{
				{"view", name},
				{"etag", etag},
				{"reduceKey", reduceKey},
				{"level", level},
				{"sourceBucket", sourceBucket},
				{"bucket", bucket},
				{"timestamp", SystemTime.UtcNow}
			}, ms.ToArray());
		}

		public void RemoveReduceResults(string indexName, int level, string reduceKey, int sourceBucket)
		{
			var results = storage.ReduceResults["ByViewReduceKeyAndSourceBucket"].SkipTo(new RavenJObject
			{
				{"view", indexName},
				{"reduceKey", reduceKey},
				{"level", level},
				{"sourceBucket", sourceBucket},
			}).TakeWhile(x => string.Equals(indexName, x.Value<string>("view"), StringComparison.InvariantCultureIgnoreCase) &&
								string.Equals(reduceKey, x.Value<string>("reduceKey"), StringComparison.InvariantCultureIgnoreCase) &&
								sourceBucket == x.Value<int>("sourceBucket") &&
								level == x.Value<int>("level"));

			foreach (var result in results)
			{
				storage.ReduceResults.Remove(result);
			}
		}

		public IEnumerable<string> GetKeysForIndexForDebug(string indexName, int start, int take)
		{
			return storage.MappedResults["ByViewReduceKeyAndBucket"].SkipTo(new RavenJObject
			{
				{"view", indexName},
			}).TakeWhile(x => string.Equals(indexName, x.Value<string>("view"), StringComparison.InvariantCultureIgnoreCase))
				.Select(x => x.Value<string>("reduceKey"))
				.Distinct()
				.Skip(start)
				.Take(take);
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(string indexName, string key, int take)
		{
			var results = storage.MappedResults["ByViewReduceKeyAndBucket"].SkipTo(new RavenJObject
			{
				{"view", indexName},
				{"reduceKey", key},
			}).TakeWhile(x => string.Equals(indexName, x.Value<string>("view"), StringComparison.InvariantCultureIgnoreCase) &&
							  string.Equals(key, x.Value<string>("reduceKey"), StringComparison.InvariantCultureIgnoreCase))
				.Take(take);

			return from result in results
				   select storage.MappedResults.Read(result)
					   into readResult
					   where readResult != null
					   select new MappedResultInfo
					   {
						   ReduceKey = readResult.Key.Value<string>("reduceKey"),
						   Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
						   Timestamp = readResult.Key.Value<DateTime>("timestamp"),
						   Bucket = readResult.Key.Value<int>("bucket"),
						   Source = readResult.Key.Value<string>("docId"),
						   Size = readResult.Size,
						   Data = LoadMappedResult(readResult)
					   };
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string indexName, string key, int level, int take)
		{
			var results = storage.ReduceResults["ByViewReduceKeyLevelAndBucket"].SkipTo(new RavenJObject
			{
				{"view", indexName},
				{"reduceKey", key},
				{"level", level}
			}).TakeWhile(x => string.Equals(indexName, x.Value<string>("view"), StringComparison.InvariantCultureIgnoreCase) &&
							  string.Equals(key, x.Value<string>("reduceKey"), StringComparison.InvariantCultureIgnoreCase) &&
							  level == x.Value<int>("level"))
				.Take(take);

			return from result in results
				   select storage.ReduceResults.Read(result)
					   into readResult
					   where readResult != null
					   select new MappedResultInfo
					   {
						   ReduceKey = readResult.Key.Value<string>("reduceKey"),
						   Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
						   Timestamp = readResult.Key.Value<DateTime>("timestamp"),
						   Bucket = readResult.Key.Value<int>("bucket"),
						   Source = readResult.Key.Value<string>("docId"),
						   Size = readResult.Size,
						   Data = LoadMappedResult(readResult)
					   };
		}
	}
}
