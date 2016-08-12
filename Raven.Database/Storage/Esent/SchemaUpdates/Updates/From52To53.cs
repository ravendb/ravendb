// -----------------------------------------------------------------------
//  <copyright file="From52To53.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Storage.Esent;
using Raven.Storage.Esent.SchemaUpdates;

namespace Raven.Database.Storage.Esent.SchemaUpdates.Updates
{
    public class From52To53 : ISchemaUpdate
    {
        private InMemoryRavenConfiguration configuration;

        public string FromSchemaVersion { get { return "5.2"; } }

        public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            using (var tbl = new Table(session, dbid, "tasks", OpenTableGrbit.None))
            {
                int rows = 0;
                if (Api.TryMoveFirst(session, tbl))
                {
                    var taskTypeColumnId = Api.GetTableColumnid(session, tbl, "task_type");
                    do
                    {
                        using (var update = new Update(session, tbl, JET_prep.Replace))
                        {
                            var taskType = Api.RetrieveColumnAsString(session, tbl, taskTypeColumnId, Encoding.Unicode);
                            Api.SetColumn(session, tbl, taskTypeColumnId, taskType, Encoding.ASCII);
                            update.Save();
                        }

                        if (rows++ % 10000 == 0)
                        {
                            output("Processed " + (rows) + " rows in tasks");
                            Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush);
                            Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
                        }
                    } while (Api.TryMoveNext(session, tbl));
                }

                SchemaCreator.UpdateVersion(session, dbid, "5.3");
            }
        }
    }
}
