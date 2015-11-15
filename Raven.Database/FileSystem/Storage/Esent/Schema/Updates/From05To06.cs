using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;

namespace Raven.Database.FileSystem.Storage.Esent.Schema.Updates
{
    public class From05To06 : IFileSystemSchemaUpdate
    {
        public string FromSchemaVersion
        {
            get { return "0.5"; }
        }
        public void Init(InMemoryRavenConfiguration configuration)
        {
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            using (var tbl = new Table(session, dbid, "files", OpenTableGrbit.None))
            {
                var indexDef = "+name\0\0";
                Api.JetDeleteIndex(session, tbl, "by_name");
                Api.JetCreateIndex2(session, tbl, new[]
                {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_name",
                        grbit = CreateIndexGrbit.IndexUnique,
                        cbKey = indexDef.Length,
                        szKey = indexDef,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        ulDensity = 80
                    }
                }, 1);
            }
            SchemaCreator.UpdateVersion(session, dbid, "0.6");
        }
    }
}
