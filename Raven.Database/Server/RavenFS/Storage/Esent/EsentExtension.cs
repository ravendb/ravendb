using System;

using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Server.RavenFS.Storage.Esent
{
	public static class EsentExtension
	{
		public static void WithDatabase(this JET_INSTANCE instance, string database, Action<Session, JET_DBID> action)
		{
			using (var session = new Session(instance))
			using (var tx = new Transaction(session))
			{
				JET_DBID dbid;
				Api.JetOpenDatabase(session, database, "", out dbid, OpenDatabaseGrbit.None);
				try
				{
					action(session, dbid);
				}
				finally
				{
					Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
				}
				tx.Commit(CommitTransactionGrbit.None);
			}
		}
	}
}
