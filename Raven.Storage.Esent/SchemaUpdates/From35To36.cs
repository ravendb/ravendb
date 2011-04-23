using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From35To36 : ISchemaUpdate
	{
		#region ISchemaUpdate Members

		public string FromSchemaVersion
		{
			get { return "3.5"; }
		}

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			Transaction tx;
			using (tx = new Transaction(session))
			{
				CreateIndexingEtagsTable(dbid, session);

				using (var stats = new Table(session, dbid, "indexes_stats",OpenTableGrbit.None))
				using (var reduce = new Table(session, dbid, "indexes_etag", OpenTableGrbit.None))
				{
					var tblKeyColumn = Api.GetColumnDictionary(session, stats)["key"];
					var reduceKeyCol = Api.GetColumnDictionary(session, reduce)["key"];

					Api.MoveBeforeFirst(session, stats);
					while (Api.TryMoveNext(session, stats))
					{
						using(var update = new Update(session, reduce, JET_prep.Insert))
						{
							var indexName = Api.RetrieveColumnAsString(session, stats, tblKeyColumn, Encoding.Unicode);
							Api.SetColumn(session, reduce, reduceKeyCol, indexName, Encoding.Unicode);
							update.Save();
						}
					}
				}

				tx.Commit(CommitTransactionGrbit.LazyFlush);
				tx.Dispose();
				tx = new Transaction(session);
			}

			using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
			{
				Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
				var columnids = Api.GetColumnDictionary(session, details);

				using (var update = new Update(session, details, JET_prep.Replace))
				{
					Api.SetColumn(session, details, columnids["schema_version"], "3.6", Encoding.Unicode);

					update.Save();
				}
			}
			tx.Commit(CommitTransactionGrbit.None);
			tx.Dispose();
		}

		private static void CreateIndexingEtagsTable(JET_DBID dbid, JET_SESID session)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "indexes_etag", 16, 100, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			var defaultValue = BitConverter.GetBytes(0);
			Api.JetAddColumn(session, tableid, "touches", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
			}, defaultValue, defaultValue.Length, out columnid);

			const string indexDef = "+key\0\0";
			Api.JetCreateIndex(session, tableid, "by_key", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   100);
		}


		#endregion
	}
}
