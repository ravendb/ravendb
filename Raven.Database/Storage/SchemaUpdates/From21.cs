using System;
using System.ComponentModel.Composition;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Storage.SchemaUpdates
{
	public class From21 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "2.1"; }
		}

		public void Update(Session session, JET_DBID dbid)
		{
			new SchemaCreator(session).CreateDirectoriesTable(dbid);

			using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
			{
				var columnids = Api.GetColumnDictionary(session, details);
				Api.TryMoveFirst(session, details);
				using (var update = new Update(session, details, JET_prep.Replace))
				{
					Api.SetColumn(session, details, columnids["schema_version"], "2.2", Encoding.Unicode);
					update.Save();
				}
			}
		}
	}
}