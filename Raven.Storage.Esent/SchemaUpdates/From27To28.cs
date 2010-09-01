using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Extensions;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From27To28 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "2.7"; }
		}

		public void Update(Session session, JET_DBID dbid)
		{
			using (var tx = new Transaction(session))
			{
				using (var indexStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None))
				{
					Api.JetDeleteColumn(session, indexStats, "last_indexed_etag");

					var defaultValue = Encoding.ASCII.GetBytes(Guid.Empty.TransformToValueForEsentSorting());
					JET_COLUMNID columnid;
					Api.JetAddColumn(session, indexStats, "last_indexed_etag", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Text,
						cp = JET_CP.ASCII,
						cbMax = 32,
						grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
					}, defaultValue, defaultValue.Length, out columnid);
				}

				using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
				{
					Api.JetDeleteIndex(session, files, "by_etag");
					Api.JetDeleteColumn(session, files, "etag");
					var defaultValue = Encoding.ASCII.GetBytes("00000000000000000000000000000001");
					JET_COLUMNID columnid;
					Api.JetAddColumn(session, files, "etag", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Text,
						cp = JET_CP.ASCII,
						cbMax = 32,
						grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
					}, defaultValue, defaultValue.Length, out columnid);

					const string indexDef = "+etag\0\0";
					Api.JetCreateIndex(session, files, "by_etag", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
									   100);
				}

				using (var documents = new Table(session, dbid, "documents", OpenTableGrbit.None))
				{
					Api.JetDeleteIndex(session, documents, "by_etag");
					Api.JetDeleteColumn(session, documents, "etag");
					var defaultValue = Encoding.ASCII.GetBytes("00000000000000000000000000000001");
					JET_COLUMNID columnid;
					Api.JetAddColumn(session, documents, "etag", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Text,
						cp = JET_CP.ASCII,
						cbMax = 32,
						grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
					}, defaultValue, defaultValue.Length, out columnid);
					const string indexDef = "+etag\0\0";
					Api.JetCreateIndex(session, documents, "by_etag", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
									   100);
				}

				using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
				{
					Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
					var columnids = Api.GetColumnDictionary(session, details);

					using (var update = new Update(session, details, JET_prep.Replace))
					{
						Api.SetColumn(session, details, columnids["schema_version"], "2.7", Encoding.Unicode);

						update.Save();
					}
				}
				tx.Commit(CommitTransactionGrbit.None);
			}
		}
	}
}