// -----------------------------------------------------------------------
//  <copyright file="From10To11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Raven.Json.Linq;
using Voron;

namespace Raven.Database.Storage.Voron.Schema.Updates
{
	internal class From11To12 : SchemaUpdateBase
	{
		public override string FromSchemaVersion
		{
			get { return "1.1"; }
		}
		public override string ToSchemaVersion
		{
			get { return "1.2"; }
		}

		public override void Update(TableStorage tableStorage, Action<string> output)
		{
			MigrateToStructures(tableStorage.Environment, tableStorage.IndexingStats, output, (json, structure) => structure
				.Set(IndexingWorkStatsFields.IndexId, json.Value<int>("index"))
				.Set(IndexingWorkStatsFields.CreatedTimestamp, json.Value<DateTime>("createdTimestamp").ToBinary())
				.Set(IndexingWorkStatsFields.LastIndexingTime, json.Value<DateTime>("lastIndexingTime").ToBinary())
				.Set(IndexingWorkStatsFields.IndexingAttempts, json.Value<int>("attempts"))
				.Set(IndexingWorkStatsFields.IndexingSuccesses, json.Value<int>("successes"))
				.Set(IndexingWorkStatsFields.IndexingErrors, json.Value<int>("failures")));

			MigrateToStructures(tableStorage.Environment, tableStorage.ReduceStats, output, (json, structure) =>
			{
				var hasReduce = json.Value<byte[]>("lastReducedEtag") != null;

				if (hasReduce)
				{
					structure.Set(ReducingWorkStatsFields.LastReducedEtag, json.Value<byte[]>("lastReducedEtag"))
						.Set(ReducingWorkStatsFields.LastReducedTimestamp, json.Value<DateTime>("lastReducedTimestamp").ToBinary())
						.Set(ReducingWorkStatsFields.ReduceAttempts, json.Value<int>("reduce_attempts"))
						.Set(ReducingWorkStatsFields.ReduceErrors, json.Value<int>("reduce_failures"))
						.Set(ReducingWorkStatsFields.ReduceSuccesses, json.Value<int>("reduce_successes"));
				}
				else
				{
					structure.Set(ReducingWorkStatsFields.ReduceAttempts, -1)
						.Set(ReducingWorkStatsFields.ReduceSuccesses, -1)
						.Set(ReducingWorkStatsFields.ReduceErrors, -1)
						.Set(ReducingWorkStatsFields.LastReducedEtag, Etag.InvalidEtag.ToByteArray())
						.Set(ReducingWorkStatsFields.LastReducedTimestamp, -1L);
				}
			});

			MigrateToStructures(tableStorage.Environment, tableStorage.LastIndexedEtags, output, (json, structure) => structure
				.Set(LastIndexedStatsFields.IndexId, json.Value<int>("index"))
				.Set(LastIndexedStatsFields.LastEtag, json.Value<byte[]>("lastEtag"))
				.Set(LastIndexedStatsFields.LastTimestamp, json.Value<DateTime>("lastTimestamp").ToBinary()));

			MigrateToStructures(tableStorage.Environment, tableStorage.DocumentReferences, output, (json, structure) =>
			{
				var view = json.Value<int>("view");
				var reference = json.Value<string>("ref");
				var key = json.Value<string>("key");

				structure.Set(DocumentReferencesFields.IndexId, view)
					.Set(DocumentReferencesFields.Reference, reference)
					.Set(DocumentReferencesFields.Key, key);
			});

			MigrateToStructures(tableStorage.Environment, tableStorage.MappedResults, output, (json, structure) =>
			{
				var view = json.Value<int>("view");
				var reduceKey = json.Value<string>("reduceKey");
				var etag = json.Value<byte[]>("etag");
				var timestamp = json.Value<DateTime>("timestamp");
				var bucket = json.Value<int>("bucket");
				var docId = json.Value<string>("docId");

				structure.Set(MappedResultFields.IndexId, view)
					.Set(MappedResultFields.ReduceKey, reduceKey)
					.Set(MappedResultFields.Etag, etag)
					.Set(MappedResultFields.Timestamp, timestamp.ToBinary())
					.Set(MappedResultFields.Bucket, bucket)
					.Set(MappedResultFields.DocId, docId);
			});

			UpdateSchemaVersion(tableStorage, output);
		}

		public void MigrateToStructures<T>(StorageEnvironment env, TableOfStructures<T> table, Action<string> output, Action<RavenJObject, Structure<T>> copyToStructure)
		{
			long entriesCount;
			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				entriesCount = tx.ReadTree(table.TableName).State.EntriesCount;
			}

			output(string.Format("Starting to migrate '{0}' table to use structures.", table.TableName));

			var migratedEntries = 0L;
			var keyToSeek = Slice.BeforeAllKeys;

			do
			{
				using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = txw.ReadTree(table.TableName);

					var iterator = tree.Iterate();

					if (iterator.Seek(keyToSeek) == false)
						break;

					var writtenStructsSize = 0;

					do
					{
						keyToSeek = iterator.CurrentKey;

						if (writtenStructsSize > 8 * 1024 * 1024) // 8 MB
							break;

						var result = tree.Read(iterator.CurrentKey);

						using (var stream = result.Reader.AsStream())
						{
							var jsonValue = stream.ToJObject();
							var structValue = new Structure<T>(table.Schema);

							copyToStructure(jsonValue, structValue);

							tree.WriteStruct(iterator.CurrentKey, structValue);

							migratedEntries++;
							writtenStructsSize += structValue.GetSize();
						}
					} while (iterator.MoveNext());

					txw.Commit();

					output(string.Format("{0} of {1} records processed.", migratedEntries, entriesCount));
				}
			} while (migratedEntries < entriesCount);

			output(string.Format("All records of '{0}' table have been migrated to structures.", table.TableName));
		}
	}
}