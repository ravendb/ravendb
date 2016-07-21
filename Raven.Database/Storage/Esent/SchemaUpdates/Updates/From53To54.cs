// -----------------------------------------------------------------------
//  <copyright file="From53To54.cs" company="Hibernating Rhinos LTD">
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
    public class From53To54 : ISchemaUpdate
    {
        private InMemoryRavenConfiguration configuration;

        public string FromSchemaVersion { get { return "5.3"; } }

        public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            using (var table = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
            {
                SchemaCreator.CreateIndexes(session, table, new JET_INDEXCREATE
                {
                    szIndexName = "by_view",
                    szKey = "+view\0\0",
                    grbit = CreateIndexGrbit.IndexDisallowNull
                });
            }

            SchemaCreator.UpdateVersion(session, dbid, "5.4");
        }
    }
}
