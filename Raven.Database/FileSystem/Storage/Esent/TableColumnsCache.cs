//-----------------------------------------------------------------------
// <copyright file="TableColumnsCache.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Server.RavenFS.Storage.Esent
{
	public class TableColumnsCache
	{
		public IDictionary<string, JET_COLUMNID> UsageColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> PagesColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> FilesColumns { get; set; }
		public IDictionary<string, JET_COLUMNID> DetailsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> ConfigColumns { get; set; }
		public IDictionary<string, JET_COLUMNID> SignaturesColumns { get; set; }

		public void InitColumDictionaries(JET_INSTANCE instance, string database)
		{
			using (var session = new Session(instance))
			{
				var dbid = JET_DBID.Nil;
				try
				{
					Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
					using (var usage = new Table(session, dbid, "usage", OpenTableGrbit.None))
						UsageColumns = Api.GetColumnDictionary(session, usage);
					using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
						DetailsColumns = Api.GetColumnDictionary(session, details);
					using (var pages = new Table(session, dbid, "pages", OpenTableGrbit.None))
						PagesColumns = Api.GetColumnDictionary(session, pages);
					using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
						FilesColumns = Api.GetColumnDictionary(session, files);
					using (var signatures = new Table(session, dbid, "signatures", OpenTableGrbit.None))
						SignaturesColumns = Api.GetColumnDictionary(session, signatures);
					using (var config = new Table(session, dbid, "config", OpenTableGrbit.None))
						ConfigColumns = Api.GetColumnDictionary(session, config);
				}
				finally
				{
					if (Equals(dbid, JET_DBID.Nil) == false)
						Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
				}
			}
		}
	}
}