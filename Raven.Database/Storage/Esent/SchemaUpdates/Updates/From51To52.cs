// -----------------------------------------------------------------------
//  <copyright file="From50To51.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Storage.Esent;
using Raven.Storage.Esent.SchemaUpdates;

namespace Raven.Database.Storage.Esent.SchemaUpdates.Updates
{
    public class From51To52 : ISchemaUpdate
    {
        private InMemoryRavenConfiguration configuration;

        public string FromSchemaVersion { get { return "5.1"; } }

        public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            using (var tbl = new Table(session, dbid, "tasks", OpenTableGrbit.None))
            {
                SchemaCreator.CreateIndexes(session, tbl, new JET_INDEXCREATE
                {
                    szIndexName = "by_task_type",
                    szKey = "+task_type\0\0",
                    grbit = CreateIndexGrbit.IndexIgnoreNull
                });

                SchemaCreator.UpdateVersion(session, dbid, "5.2");
            }
        }
    }
}
