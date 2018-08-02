using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;

namespace Raven.Database.FileSystem.Storage.Esent.Schema.Updates
{
    public class From07To08 : IFileSystemSchemaUpdate
    {
        public string FromSchemaVersion
        {
            get { return "0.7"; }
        }
        public void Init(InMemoryRavenConfiguration configuration)
        {
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            // this schema update is first part of migration changing autoincremented ids column type from
            // JET_coltyp.Long (int32 actually) to JET_coltyp.Currency (int64)
            // here we are updating 'usages' table
            // the reason we do it as separate migration steps is that if we fail during second phase (From08To09) then
            // we won't need to apply this one again (From07To08)
            
            var usageTableName = "usage";

            var newTableName = usageTableName + "_new";
            
            try
            {
                Api.JetDeleteTable(session, dbid, newTableName);
            }
            catch (Exception)
            {
                //if there is no such table - then it is not important
                //this is a precaution against partially failed upgrade process
            }

            SchemaCreator.CreateUsageTable(dbid, newTableName, session);


            using (var newUsage = new Table(session, dbid, newTableName, OpenTableGrbit.None))
            {
                // let's create index in usages table necessary for migration purposes in schema upgrade From08To09
                // we need to seek by page_id in order to update value in 'usages' table 
                // during migration of 'pages' table

                var indexDef = "+page_id\0\0";

                Api.JetCreateIndex2(session, newUsage, new[]
                {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_page_id",
                        cbKey = indexDef.Length,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = indexDef,
                        grbit = CreateIndexGrbit.None,
                        ulDensity = 80
                    }
                }, 1);
            }

            var rows = 0;

            using (var src = new Table(session, dbid, usageTableName, OpenTableGrbit.None))
            using (var dst = new Table(session, dbid, newTableName, OpenTableGrbit.None))
            {
                Api.MoveBeforeFirst(session, src);
                Api.MoveBeforeFirst(session, dst);

                var srcColumns = Api.GetColumnDictionary(session, src);
                var dstColumns = Api.GetColumnDictionary(session, dst);

                while (Api.TryMoveNext(session, src))
                {
                    using (var insert = new Update(session, dst, JET_prep.Insert))
                    {
                        var name = Api.RetrieveColumnAsString(session, src, srcColumns["name"]);

                        var page_size = Api.RetrieveColumnAsInt32(session, src, srcColumns["page_size"]).Value;
                        var page_id = Api.RetrieveColumnAsInt32(session, src, srcColumns["page_id"]).Value;
                        var file_pos = Api.RetrieveColumnAsInt32(session, src, srcColumns["file_pos"]).Value;

                        Api.SetColumn(session, dst, dstColumns["name"], name, Encoding.Unicode);
                        Api.SetColumn(session, dst, dstColumns["file_pos"], file_pos);
                        Api.SetColumn(session, dst, dstColumns["page_id"], (long)page_id);
                        Api.SetColumn(session, dst, dstColumns["page_size"], page_size);

                        insert.Save();
                    }

                    if (rows++ % 10000 == 0)
                    {
                        output("Processed " + (rows) + " rows in '" + usageTableName + "' table");
                        Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush);
                        Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
                    }
                }
            }

            output("Processed " + (rows) + " rows in '" + usageTableName + "' table. DONE");

            Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
            Api.JetDeleteTable(session, dbid, usageTableName);
            Api.JetRenameTable(session, dbid, newTableName, usageTableName);
            Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);

            SchemaCreator.UpdateVersion(session, dbid, "0.8");
        }
    }
}
