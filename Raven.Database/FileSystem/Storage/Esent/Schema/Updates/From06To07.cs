using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;

namespace Raven.Database.FileSystem.Storage.Esent.Schema.Updates
{
	public class From06To07 : IFileSystemSchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "0.6"; }
		}
		public void Init(InMemoryRavenConfiguration configuration)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			using (var tbl = new Table(session, dbid, "usage", OpenTableGrbit.None))
			{
				var indexDef = "+name\0+file_pos\0\0";
				Api.JetDeleteIndex(session, tbl, "by_name_and_pos");
				Api.JetCreateIndex2(session, tbl, new[]
				{
					new JET_INDEXCREATE
					{
						szIndexName = "by_name_and_pos",
						cbKey = indexDef.Length,
						cbKeyMost = SystemParameters.KeyMost,
						cbVarSegMac = SystemParameters.KeyMost,
						szKey = indexDef,
						grbit = CreateIndexGrbit.None,
						ulDensity = 80
					}
				}, 1);
			}

			SchemaCreator.UpdateVersion(session, dbid, "0.7");
		}
	}
}
