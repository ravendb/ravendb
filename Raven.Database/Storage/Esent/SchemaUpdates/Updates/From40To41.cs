// -----------------------------------------------------------------------
//  <copyright file="From40To41.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	using System;
	using Database.Impl;
	using Microsoft.Isam.Esent.Interop;

	public class From40To41 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.0"; } }
		
		public void Init(IUuidGenerator generator)
		{
			
		}

		public void Update(Session session, JET_DBID dbid)
		{
			CreateReduceKeysTable(session, dbid);

			using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
			{
				SchemaCreator.CreateIndexes(session, mappedResults, new JET_INDEXCREATE
				{
					szIndexName = "by_view_and_hashed_reduce_key",
					szKey = "+view\0+hashed_reduce_key\0\0",
				});
			}

			SchemaCreator.UpdateVersion(session, dbid, "4.1");
		}

		private void CreateReduceKeysTable(Session session, JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "reduce_keys", 1, 80, out tableid);
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

			Api.JetAddColumn(session, tableid, "reduce_type", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			var defaultValue = BitConverter.GetBytes(0);
			Api.JetAddColumn(session, tableid, "mapped_items_count", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
			}, defaultValue, defaultValue.Length, out columnid);

			CreateIndexes(session, tableid,
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
					szIndexName = "by_view_and_reduce_key",
					szKey = "+view\0+reduce_key\0\0",
				});
		}

		public static void CreateIndexes(Session session, JET_TABLEID tableid, params JET_INDEXCREATE[] indexes)
		{
			foreach (var index in indexes)
			{
				try
				{
					Api.JetCreateIndex(session, tableid, index.szIndexName, index.grbit, index.szKey, index.szKey.Length, 90);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Could not create index: " + index.szIndexName, e);
				}
			}
		}
	}
}