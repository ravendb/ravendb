// -----------------------------------------------------------------------
//  <copyright file="From47To48.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From47To48 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.7"; } }

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "etag_synchronization", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "indexer_etag", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 16,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "reducer_etag", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 16,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "replicator_etag", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 16,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "sql_replicator_etag", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 16,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			SchemaCreator.CreateIndexes(session, tableid, new JET_INDEXCREATE
			{
				szIndexName = "by_key",
				szKey = "+key\0\0",
				grbit = CreateIndexGrbit.IndexPrimary
			});

			SchemaCreator.UpdateVersion(session, dbid, "4.8");
		}
	}
}