using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Client.Document;
using Raven.Imports.SignalR.Client;
using Raven.Imports.SignalR.Client.Hubs;
using System.Reactive.Linq;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			using(var store = new DocumentStore
			{
				Url = "http://localhost:9001"
			}.Initialize())
			{
				store.Changes().Subscribe(notification => Console.WriteLine(notification.Name));



				Console.ReadLine();
			}

		}


		private static int TranslateToSizeInDatabasePages(int sizeInMegabytes, int multiply)
		{
			//This doesn't suffer from overflow, do the division first (to make the number smaller) then multiply afterwards
			double tempAmt = (double)sizeInMegabytes / SystemParameters.DatabasePageSize;
			int finalSize = (int)(tempAmt * multiply);
			return finalSize;
		}
		private static void CreateDocumentsTable(JET_DBID dbid, Session session)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "documents", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 2048,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "etag", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "last_modified", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.DateTime,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "locked_by_transaction", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnTagged,
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "metadata", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			CreateIndexes(tableid, session,
						  new JET_INDEXCREATE
						  {
							  szIndexName = "by_id",
							  szKey = "+id\0\0",
							  grbit = CreateIndexGrbit.IndexPrimary
						  },
						  new JET_INDEXCREATE
						  {
							  szIndexName = "by_etag",
							  szKey = "+etag\0\0",
							  grbit = CreateIndexGrbit.IndexDisallowNull
						  },
						  new JET_INDEXCREATE
						  {
							  szIndexName = "by_key",
							  szKey = "+key\0\0",
							  grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
						  });
		}


		private static void CreateIndexes(JET_TABLEID tableid, Session session, params JET_INDEXCREATE[] indexes)
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