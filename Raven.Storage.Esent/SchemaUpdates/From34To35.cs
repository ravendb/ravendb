using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From34To35 : ISchemaUpdate
	{
		#region ISchemaUpdate Members

		public string FromSchemaVersion
		{
			get { return "3.4"; }
		}

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			Transaction tx;
			using (tx = new Transaction(session))
			{
                using (var tbl = new Table(session, dbid, "indexes_stats_reduce",
					OpenTableGrbit.PermitDDL | OpenTableGrbit.DenyRead | OpenTableGrbit.DenyWrite))
				{
				    JET_COLUMNID columnid;
				    Api.JetAddColumn(session, tbl, "last_reduced_etag", new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.Binary,
                        cbMax = 16,
                        grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
                    }, null, 0, out columnid);


                    Api.JetAddColumn(session, tbl, "last_reduced_timestamp", new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.DateTime,
                        grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
                    }, null, 0, out columnid);
				}

				tx.Commit(CommitTransactionGrbit.LazyFlush);
				tx.Dispose();
				tx = new Transaction(session);
			}

            using (var tbl = new Table(session, dbid, "mapped_results",
                    OpenTableGrbit.PermitDDL | OpenTableGrbit.DenyRead | OpenTableGrbit.DenyWrite))
            {
                const string indexDef = "+view\0+etag\0\0";
                Api.JetCreateIndex(session, tbl, "by_view_and_etag", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                                   100);

            }

			using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
			{
				Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
				var columnids = Api.GetColumnDictionary(session, details);

				using (var update = new Update(session, details, JET_prep.Replace))
				{
					Api.SetColumn(session, details, columnids["schema_version"], "3.5", Encoding.Unicode);

					update.Save();
				}
			}
			tx.Commit(CommitTransactionGrbit.None);
			tx.Dispose();
		}
		
        #endregion
	}
}
