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
			//TODO arek - add output info

			using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var indexingStats = tx.ReadTree(Tables.IndexingStats.TableName);

				var iterator = indexingStats.Iterate();

				if (iterator.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						var result = indexingStats.Read(iterator.CurrentKey);

						using (var stream = result.Reader.AsStream())
						{
							var indexStats = stream.ToJObject();

							var statsStructure = new Structure<IndexingWorkStatsFields>(tableStorage.IndexingStats.Schema)
								.Set(IndexingWorkStatsFields.IndexId, indexStats.Value<int>("index"))
								.Set(IndexingWorkStatsFields.CreatedTimestamp, indexStats.Value<DateTime>("createdTimestamp").ToBinary())
								.Set(IndexingWorkStatsFields.LastIndexingTime, indexStats.Value<DateTime>("lastIndexingTime").ToBinary())
								.Set(IndexingWorkStatsFields.IndexingAttempts, indexStats.Value<int>("attempts"))
								.Set(IndexingWorkStatsFields.IndexingSuccesses, indexStats.Value<int>("successes"))
								.Set(IndexingWorkStatsFields.IndexingErrors, indexStats.Value<int>("failures"));

							indexingStats.WriteStruct(iterator.CurrentKey, statsStructure);
						}

					} while (iterator.MoveNext());
				}

				tx.Commit();
			}

			using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var reducingStats = tx.ReadTree(Tables.ReduceStats.TableName);

				var iterator = reducingStats.Iterate();

				if (iterator.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						var result = reducingStats.Read(iterator.CurrentKey);

						using (var stream = result.Reader.AsStream())
						{
							var reduceStats = stream.ToJObject();

							var hasReduce = reduceStats.Value<byte[]>("lastReducedEtag") != null;

							var voronStats = new Structure<ReducingWorkStatsFields>(tableStorage.ReduceStats.Schema);

							if (hasReduce)
							{
								voronStats.Set(ReducingWorkStatsFields.LastReducedEtag, reduceStats.Value<byte[]>("lastReducedEtag"))
									.Set(ReducingWorkStatsFields.LastReducedTimestamp, reduceStats.Value<DateTime>("lastReducedTimestamp").ToBinary())
									.Set(ReducingWorkStatsFields.ReduceAttempts, reduceStats.Value<int>("reduce_attempts"))
									.Set(ReducingWorkStatsFields.ReduceErrors, reduceStats.Value<int>("reduce_failures"))
									.Set(ReducingWorkStatsFields.ReduceSuccesses, reduceStats.Value<int>("reduce_successes"));
							}
							else
							{
								voronStats.Set(ReducingWorkStatsFields.ReduceAttempts, -1)
									.Set(ReducingWorkStatsFields.ReduceSuccesses, -1)
									.Set(ReducingWorkStatsFields.ReduceErrors, -1)
									.Set(ReducingWorkStatsFields.LastReducedEtag, Etag.InvalidEtag.ToByteArray())
									.Set(ReducingWorkStatsFields.LastReducedTimestamp, -1L);
							}

							reducingStats.WriteStruct(iterator.CurrentKey, voronStats);
						}

					} while (iterator.MoveNext());
				}

				tx.Commit();
			}

			using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var lastIndexedEtags = tx.ReadTree(Tables.LastIndexedEtags.TableName);

				var iterator = lastIndexedEtags.Iterate();

				if (iterator.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						var result = lastIndexedEtags.Read(iterator.CurrentKey);

						using (var stream = result.Reader.AsStream())
						{
							var stats = stream.ToJObject();

							var voronStats = new Structure<LastIndexedStatsFields>(tableStorage.LastIndexedEtags.Schema);

							voronStats.Set(LastIndexedStatsFields.IndexId, stats.Value<int>("index"))
								.Set(LastIndexedStatsFields.LastEtag, stats.Value<byte[]>("lastEtag"))
								.Set(LastIndexedStatsFields.LastTimestamp, stats.Value<DateTime>("lastTimestamp").ToBinary());

							lastIndexedEtags.WriteStruct(iterator.CurrentKey, voronStats);
						}
					} while (iterator.MoveNext());
				}

				tx.Commit();
			}

			using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var documentReferences = tx.ReadTree(Tables.DocumentReferences.TableName);

				var iterator = documentReferences.Iterate();

				if (iterator.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						var result = documentReferences.Read(iterator.CurrentKey);

						using (var stream = result.Reader.AsStream())
						{
							var value = stream.ToJObject();

							var view = value.Value<int>("view");
							var reference = value.Value<string>("ref");
							var key = value.Value<string>("key");

							var voronValue = new Structure<DocumentReferencesFields>(tableStorage.DocumentReferences.Schema);

							voronValue.Set(DocumentReferencesFields.IndexId, view)
								.Set(DocumentReferencesFields.Reference, reference)
								.Set(DocumentReferencesFields.Key, key);
							
							documentReferences.WriteStruct(iterator.CurrentKey, voronValue);
						}
					} while (iterator.MoveNext());
				}
			}

			UpdateSchemaVersion(tableStorage, output);
		}
	}
}