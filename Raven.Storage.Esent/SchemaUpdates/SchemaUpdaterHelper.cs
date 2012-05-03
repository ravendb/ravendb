using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public static class SchemaUpdaterHelper
	{
		public static void UpdateSchemaVersion(this Session session, JET_DBID dbid, string schemaVersion)
		{
			using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
			{
				Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
				var columnids = Api.GetColumnDictionary(session, details);

				using (var update = new Update(session, details, JET_prep.Replace))
				{
					Api.SetColumn(session, details, columnids["schema_version"], schemaVersion, Encoding.Unicode);

					update.Save();
				}
			}
		}
	}
}