using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From26To27 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "2.6"; }
		}

        private IUuidGenerator uuidGenerator;

        public void Init(IUuidGenerator generator)
        {
            uuidGenerator = generator;
        }

		public void Update(Session session, JET_DBID dbid)
		{
			using (var tx = new Transaction(session))
			{
				using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
				{

					const string indexDef = "+etag\0\0";
					Api.JetCreateIndex(session, files, "by_etag", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
									   100);

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
				}
				tx.Commit(CommitTransactionGrbit.None);
			}
		}
	}
}
