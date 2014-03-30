// -----------------------------------------------------------------------
//  <copyright file="From43To44.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From43To44 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "4.3"; }
		}

		public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			CreateIndexedDocumentsReferencesTable(session, dbid);
			SchemaCreator.UpdateVersion(session, dbid, "4.4");
		}

		private void CreateIndexedDocumentsReferencesTable(Session session, JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "indexed_documents_references", 1, 80, out tableid);
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

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "ref", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = SchemaCreator.ColumnNotNullIfOnHigherThanWindowsXp()
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
					szIndexName = "by_key",
					szKey = "+key\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_view",
					szKey = "+key\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_view_and_key",
					szKey = "+view\0+key\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull
				},
				new JET_INDEXCREATE
				{
					szIndexName = "by_ref",
					szKey = "+ref\0\0",
					grbit = CreateIndexGrbit.IndexDisallowNull
				});
		}

	}
}