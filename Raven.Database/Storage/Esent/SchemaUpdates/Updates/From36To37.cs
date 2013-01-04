// -----------------------------------------------------------------------
//  <copyright file="From36To37.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From36To37 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "3.6"; }
		}

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			CreateListsTable(session, dbid);
			SchemaCreator.UpdateVersion(session, dbid, "3.7");
		}

		public void CreateListsTable(Session session, JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "lists", 1, 80, out tableid);
			JET_COLUMNID columnid;


			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "etag", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);


			SchemaCreator.CreateIndexes(session, tableid,
				new JET_INDEXCREATE
				{
					szIndexName = "by_id",
					szKey = "+id\0\0",
					grbit = CreateIndexGrbit.IndexPrimary
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_name_and_etag",
					szKey = "+name\0+etag\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_name_and_key",
					szKey = "+name\0+key\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique
				});
		
		}

	}
}