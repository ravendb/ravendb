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
            var usageTableName = "usage";

            var newTableName = usageTableName + "_new";

            JET_TABLEID newTableId;
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
                        Api.SetColumn(session, dst, dstColumns["page_id"], page_id);
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

            Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
            Api.JetDeleteTable(session, dbid, usageTableName);
            Api.JetRenameTable(session, dbid, newTableName, usageTableName);
            Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);

            SchemaCreator.UpdateVersion(session, dbid, "0.8");
        }
    }
}
