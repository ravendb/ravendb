using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From25To26 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "2.5"; }
		}

		public void Update(Session session, JET_DBID dbid)
		{
			using (var tx = new Transaction(session))
			{
				using (var indexStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None))
				{
					var defaultValue = Guid.Empty.ToByteArray();
					JET_COLUMNID columnid;
					Api.JetAddColumn(session, indexStats, "last_indexed_etag", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Bit,
						cbMax = 16,
						grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
					}, defaultValue, defaultValue.Length, out columnid);

					using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
					{
						Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
						var columnids = Api.GetColumnDictionary(session, details);

						using (var update = new Update(session, details, JET_prep.Replace))
						{
							Api.SetColumn(session, details, columnids["schema_version"], "2.6", Encoding.Unicode);

							update.Save();
						}
					}
				}
				tx.Commit(CommitTransactionGrbit.None);
			}
		}
	}
}