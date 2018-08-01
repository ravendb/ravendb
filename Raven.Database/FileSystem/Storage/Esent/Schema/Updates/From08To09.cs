using System;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;

namespace Raven.Database.FileSystem.Storage.Esent.Schema.Updates
{
    public class From08To09 : IFileSystemSchemaUpdate
    {
        public string FromSchemaVersion
        {
            get { return "0.8"; }
        }
        public void Init(InMemoryRavenConfiguration configuration)
        {
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            // this is continuation of migration started in From07To08
            // here we are migrating 'pages' table and update relevant rows in 'usages'

            var pagesTableName = "pages";

            var newTableName = pagesTableName + "_new";

            try
            {
                Api.JetDeleteTable(session, dbid, newTableName);
            }
            catch (Exception)
            {
                //if there is no such table - then it is not important
                //this is a precaution against partially failed upgrade process
            }

            SchemaCreator.CreatePagesTable(dbid, newTableName, session);

            var buffer = new byte[StorageConstants.MaxPageSize];
            var bookMarkBuffer = new byte[SystemParameters.BookmarkMost];

            using (var usage = new Table(session, dbid, "usage", OpenTableGrbit.None))
            {
                var usageColumns = Api.GetColumnDictionary(session, usage);

                using (var src = new Table(session, dbid, pagesTableName, OpenTableGrbit.None))
                using (var dst = new Table(session, dbid, newTableName, OpenTableGrbit.None))
                { 
                    Api.MoveBeforeFirst(session, src);
                    Api.MoveBeforeFirst(session, dst);

                    var srcColumns = Api.GetColumnDictionary(session, src);
                    var dstColumns = Api.GetColumnDictionary(session, dst);

                    int rows = 0;

                    while (Api.TryMoveNext(session, src))
                    {
                        using (var insert = new Update(session, dst, JET_prep.Insert))
                        {
                            var oldPageId = Api.RetrieveColumnAsInt32(session, src, srcColumns["id"]).Value;

                            // page_strong_hash

                            using (var srcStream = new BufferedStream(new ColumnStream(session, src, srcColumns["page_strong_hash"])))
                            using (var dstStream = new ColumnStream(session, dst, dstColumns["page_strong_hash"]))
                            {
                                int readCount;
                                int offset = 0;

                                while ((readCount = srcStream.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    dstStream.Write(buffer, offset, readCount);

                                    offset += readCount;
                                }

                                dstStream.Flush();
                            }

                            // page_weak_hash

                            var page_weak_hash = Api.RetrieveColumnAsInt32(session, src, srcColumns["page_weak_hash"]).Value;
                            Api.SetColumn(session, dst, dstColumns["page_weak_hash"], page_weak_hash);

                            // data

                            using (var srcStream = new BufferedStream(new ColumnStream(session, src, srcColumns["data"])))
                            using (var dstStream = new ColumnStream(session, dst, dstColumns["data"]))
                            {
                                int readCount;
                                int offset = 0;

                                while ((readCount = srcStream.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    dstStream.Write(buffer, offset, readCount);

                                    offset += readCount;
                                }

                                dstStream.Flush();
                            }

                            // usage_count

                            var usage_count = Api.RetrieveColumnAsInt32(session, src, srcColumns["usage_count"]).Value;
                            Api.SetColumn(session, dst, dstColumns["usage_count"], usage_count);

                            var actualSize = 0;

                            insert.Save(bookMarkBuffer, bookMarkBuffer.Length, out actualSize);

                            Api.JetGotoBookmark(session, dst, bookMarkBuffer, actualSize);

                            var newPageId = Api.RetrieveColumnAsInt64(session, dst, dstColumns["id"]).Value;

                            Api.JetSetCurrentIndex(session, usage, "by_page_id");

                            Api.MakeKey(session, usage, (long)oldPageId, MakeKeyGrbit.NewKey);

                            if (Api.TrySeek(session, usage, SeekGrbit.SeekEQ))
                            {
                                do
                                {
                                    var page_id = Api.RetrieveColumnAsInt64(session, usage, usageColumns["page_id"]).Value;

                                    if (page_id != oldPageId)
                                        break;

                                    using (var update = new Update(session, usage, JET_prep.Replace))
                                    {
                                        Api.SetColumn(session, usage, usageColumns["page_id"], newPageId);

                                        update.Save();
                                    }

                                } while (Api.TryMoveNext(session, usage));
                            }
                        }

                        if (rows++ % 10000 == 0)
                        {
                            output("Processed " + (rows) + " rows in 'pages' table");
                            Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush);
                            Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
                        }
                    }
                }
            }

            using (var usage = new Table(session, dbid, "usage", OpenTableGrbit.None))
            {
                // delete no longer necessary index, created in schema upgrade From07To08

                Api.JetDeleteIndex(session, usage, "by_page_id");
            }

            Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
            Api.JetDeleteTable(session, dbid, pagesTableName);
            Api.JetRenameTable(session, dbid, newTableName, pagesTableName);
            Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);

            SchemaCreator.UpdateVersion(session, dbid, "0.9");
        }
    }
}
