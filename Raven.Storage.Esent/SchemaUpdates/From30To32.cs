using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Extensions;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates
{
    public class From30To32 : ISchemaUpdate
    {
        #region ISchemaUpdate Members

        public string FromSchemaVersion
        {
            get { return "3.0"; }
        }

        public void Init(IUuidGenerator generator)
        {
        }

        public void Update(Session session, JET_DBID dbid)
        {
            Transaction tx;
            using (tx = new Transaction(session))
            {
                var count = 0;
                const int rowsInTxCount = 100;
                var tablesWithEtags = new[]
                {
                    new {Table = "indexes_stats", Column = "last_indexed_etag"},
                    new {Table = "documents", Column = "etag"},
                    new {Table = "mapped_results", Column = "etag"},
                    new {Table = "files", Column = "etag"},
                    new {Table = "documents_modified_by_transaction", Column = "etag"},
                };
                foreach (var tablesWithEtag in tablesWithEtags)
                {
                    using (var tbl = new Table(session, dbid, tablesWithEtag.Table, OpenTableGrbit.None))
                    {
                        var columnid = Api.GetColumnDictionary(session, tbl)[tablesWithEtag.Column];
                        Api.MoveBeforeFirst(session, tbl);
                        while (Api.TryMoveNext(session, tbl))
                        {
                            using (var update = new Update(session, tbl, JET_prep.Replace))
                            {
                                Api.SetColumn(session, tbl, columnid, Guid.Empty.TransformToValueForEsentSorting());
                                update.Save();
                            }
                            if (count++%rowsInTxCount == 0)
                            {
                                tx.Commit(CommitTransactionGrbit.LazyFlush);
                                tx.Dispose();
                                tx = new Transaction(session);
                            }
                        }
                        tx.Commit(CommitTransactionGrbit.LazyFlush);
                        tx.Dispose();
                        tx = new Transaction(session);
                    }
                }
                tx.Commit(CommitTransactionGrbit.LazyFlush);
                tx.Dispose();
            }
        }

        #endregion
    }
}