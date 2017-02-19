// -----------------------------------------------------------------------
//  <copyright file="From50To51.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Storage.Esent;
using Raven.Storage.Esent.SchemaUpdates;

namespace Raven.Database.Storage.Esent.SchemaUpdates.Updates
{
    public class From50To51 : ISchemaUpdate
    {
        private InMemoryRavenConfiguration configuration;

        public string FromSchemaVersion { get { return "5.0"; } }

        public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            using (var tbl = new Table(session, dbid, "lists", OpenTableGrbit.None))
            {
                JET_COLUMNID columnid;
                var columnids = Api.GetColumnDictionary(session, tbl);
                if (columnids.ContainsKey("created_at") == false)
                {
                    Api.JetAddColumn(session, tbl, "created_at", new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.DateTime,
                        grbit = ColumndefGrbit.ColumnMaybeNull,
                    }, null, 0, out columnid);
                }
                int rows = 0;
                if (Api.TryMoveFirst(session, tbl))
                {
                    do
                    {
                        using (var update = new Update(session, tbl, JET_prep.Replace))
                        {
                            var createdAt = Api.GetTableColumnid(session, tbl, "created_at");
                            Api.SetColumn(session, tbl, createdAt, SystemTime.UtcNow);
                            update.Save();
                        }
                        if (rows++ % 10000 == 0)
                        {
                            output("Processed " + (rows) + " rows in lists");
                            Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush);
                            Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
                        }
                    } while (Api.TryMoveNext(session, tbl));
                }

                SchemaCreator.CreateIndexes(session, tbl, new JET_INDEXCREATE
                {
                    szIndexName = "by_name_and_created_at",
                    szKey = "+name\0+created_at\0\0",
                    grbit = CreateIndexGrbit.IndexDisallowNull
                });

                SchemaCreator.UpdateVersion(session, dbid, "5.1");
            }
        }
    }
}
