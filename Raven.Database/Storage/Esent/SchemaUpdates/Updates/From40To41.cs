// -----------------------------------------------------------------------
//  <copyright file="From40To41.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From40To41 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.0"; } }

		public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
		{

		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			var i = 0;

			CreateReduceKeysCountsTable(session,dbid);
			CreateReduceKeysStatusTable(session, dbid);

			var countsPerKeyPerIndex = new Dictionary<string, Dictionary<string, int>>(StringComparer.OrdinalIgnoreCase);
			using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
			{
				SchemaCreator.CreateIndexes(session, mappedResults, new JET_INDEXCREATE
				{
					szIndexName = "by_view_and_hashed_reduce_key",
					szKey = "+view\0+hashed_reduce_key\0\0",
				});

				var columnDictionary = Api.GetColumnDictionary(session, mappedResults);
				Api.MoveBeforeFirst(session, mappedResults);
				while (Api.TryMoveNext(session, mappedResults))
				{
					var index = Api.RetrieveColumnAsString(session, mappedResults, columnDictionary["view"], Encoding.Unicode);
					var reduceKey = Api.RetrieveColumnAsString(session, mappedResults, columnDictionary["reduce_key"], Encoding.Unicode);
					var countPerKey = countsPerKeyPerIndex.GetOrAdd(index);
					countPerKey[reduceKey] = countPerKey.GetOrDefault(reduceKey) + 1;

					if (i++%10000 == 0)
						output("Processed " + (i - 1) + " rows in mapped_results");
				}
			}

			output("Finished processing mapped_results");

			using (var reduceKeys = new Table(session, dbid, "reduce_keys_status", OpenTableGrbit.None))
			{
				var columnDictionary = Api.GetColumnDictionary(session, reduceKeys);
				foreach (var countPerKey in countsPerKeyPerIndex)
				{
					foreach (var keyCount in countPerKey.Value)
					{
						using (var update = new Update(session, reduceKeys, JET_prep.Insert))
						{
							Api.SetColumn(session, reduceKeys, columnDictionary["view"], countPerKey.Key, Encoding.Unicode);
							Api.SetColumn(session, reduceKeys, columnDictionary["reduce_key"], keyCount.Key, Encoding.Unicode);
							Api.SetColumn(session, reduceKeys, columnDictionary["hashed_reduce_key"], DocumentStorageActions.HashReduceKey(keyCount.Key));
							Api.SetColumn(session, reduceKeys, columnDictionary["reduce_type"], (int)ReduceType.MultiStep);
							update.Save();
						}
					}
				}
			}

			output("Finished processing reduce_keys_status");

			using (var reduceKeys = new Table(session, dbid, "reduce_keys_counts", OpenTableGrbit.None))
			{
				var columnDictionary = Api.GetColumnDictionary(session, reduceKeys);
				foreach (var countPerKey in countsPerKeyPerIndex)
				{
					foreach (var keyCount in countPerKey.Value)
					{
						using (var update = new Update(session, reduceKeys, JET_prep.Insert))
						{
							Api.SetColumn(session, reduceKeys, columnDictionary["view"], countPerKey.Key, Encoding.Unicode);
							Api.SetColumn(session, reduceKeys, columnDictionary["reduce_key"], keyCount.Key, Encoding.Unicode);
							Api.SetColumn(session, reduceKeys, columnDictionary["hashed_reduce_key"], DocumentStorageActions.HashReduceKey(keyCount.Key));
							Api.SetColumn(session, reduceKeys, columnDictionary["mapped_items_count"], keyCount.Value);
							update.Save();
						}
					}
				}
			}

			output("Finished processing reduce_keys_counts");

			using (var scheduledReductions = new Table(session, dbid, "scheduled_reductions", OpenTableGrbit.None))
			{
				SchemaCreator.CreateIndexes(session, scheduledReductions, new JET_INDEXCREATE
				{
					szIndexName = "by_view_level_and_hashed_reduce_key",
					szKey = "+view\0+level\0+hashed_reduce_key\0\0",
				});
			}

			output("Finished processing scheduled_reductions");
		
			SchemaCreator.UpdateVersion(session, dbid, "4.1");
		}

		private void CreateReduceKeysCountsTable(Session session,JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "reduce_keys_counts", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "view", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "reduce_key", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "hashed_reduce_key", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 20,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp() | ColumndefGrbit.ColumnFixed
			}, null, 0, out columnid);

			var defaultValue = BitConverter.GetBytes(0);
			Api.JetAddColumn(session, tableid, "mapped_items_count", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
			}, defaultValue, defaultValue.Length, out columnid);

			SchemaCreator.CreateIndexes(session,tableid,
				new JET_INDEXCREATE
				{
					szIndexName = "by_id",
					szKey = "+id\0\0",
					grbit = CreateIndexGrbit.IndexPrimary
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_view",
					szKey = "+view\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_view_and_hashed_reduce_key",
					szKey = "+view\0+hashed_reduce_key\0+reduce_key\0\0",
					grbit = CreateIndexGrbit.IndexUnique
				});
		}

		private void CreateReduceKeysStatusTable(Session session, JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "reduce_keys_status", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "view", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "reduce_key", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "hashed_reduce_key", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 20,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp() | ColumndefGrbit.ColumnFixed
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "reduce_type", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			SchemaCreator.CreateIndexes(session,tableid,
				new JET_INDEXCREATE
				{
					szIndexName = "by_id",
					szKey = "+id\0\0",
					grbit = CreateIndexGrbit.IndexPrimary
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_view",
					szKey = "+view\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_view_and_hashed_reduce_key",
					szKey = "+view\0+hashed_reduce_key\0+reduce_key\0\0",
					grbit = CreateIndexGrbit.IndexUnique
				});
		}

		

	}
}