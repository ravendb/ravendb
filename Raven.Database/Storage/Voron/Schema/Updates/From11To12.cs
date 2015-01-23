// -----------------------------------------------------------------------
//  <copyright file="From10To11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Storage.Voron.StorageActions.Structs;
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

							var voronStats = new VoronIndexingWorkStats
							{
								IndexId = indexStats.Value<int>("index"),
								CreatedTimestampTicks = indexStats.Value<DateTime>("createdTimestamp").Ticks,
								LastIndexingTimeTicks = indexStats.Value<DateTime>("lastIndexingTime").Ticks,
								IndexingAttempts = indexStats.Value<int>("attempts"),
								IndexingSuccesses = indexStats.Value<int>("successes"),
								IndexingErrors = indexStats.Value<int>("failures")
							};

							indexingStats.Write(iterator.CurrentKey, voronStats);
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

							VoronReducingWorkStats voronStats;

							if (hasReduce)
							{
								voronStats = new VoronReducingWorkStats
								{
									LastReducedEtag = new VoronEtagStruct(Etag.Parse(reduceStats.Value<byte[]>("lastReducedEtag"))),
									LastReducedTimestampTicks = reduceStats.Value<DateTime>("lastReducedTimestamp").Ticks,
									ReduceAttempts = reduceStats.Value<int>("reduce_attempts"),
									ReduceErrors = reduceStats.Value<int>("reduce_failures"),
									ReduceSuccesses = reduceStats.Value<int>("reduce_successes")
								};
							}
							else
							{
								voronStats = new VoronReducingWorkStats
								{
									ReduceAttempts = -1,
									ReduceSuccesses = -1,
									ReduceErrors = -1,
									LastReducedEtag = new VoronEtagStruct(Etag.InvalidEtag),
									LastReducedTimestampTicks = -1
								};
							}

							reducingStats.Write(iterator.CurrentKey, voronStats);
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

							var voronStats = new VoronLastIndexedStats
							{
								IndexId = stats.Value<int>("index"),
								LastEtag = new VoronEtagStruct(Etag.Parse(stats.Value<byte[]>("lastEtag"))),
								LastTimestampTicks = stats.Value<DateTime>("lastTimestamp").Ticks
							};

							lastIndexedEtags.Write(iterator.CurrentKey, voronStats);
						}
					} while (iterator.MoveNext());
				}

				tx.Commit();
			}

			UpdateSchemaVersion(tableStorage, output);
		}
	}
}