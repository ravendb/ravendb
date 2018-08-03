// -----------------------------------------------------------------------
//  <copyright file="From54To55.cs" company="Hibernating Rhinos LTD">
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
    public class From54To55 : ISchemaUpdate
    {
        private InMemoryRavenConfiguration configuration;

        public string FromSchemaVersion { get { return "5.4"; } }

        public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void Update(Session session, JET_DBID dbid, Action<string> output)
        {
            using (var tbl = new Table(session, dbid, "lists", OpenTableGrbit.None))
            {
                var by_id = "+id\0\0";
                var by_name_and_etag = "+name\0+etag\0\0";
                var by_name_and_key = "+name\0+key\0\0";
                var by_name_and_created_at = "+name\0+created_at\0\0";

                var tableid = tbl.JetTableid;

                Api.JetDeleteIndex(session, tbl, "szIndexName");
                Api.JetDeleteIndex(session, tbl, "by_name_and_etag");
                Api.JetDeleteIndex(session, tbl, "by_name_and_key");
                Api.JetDeleteIndex(session, tbl, "by_name_and_created_at");

                Api.JetCreateIndex2(session, tableid, new[]
                {
                new JET_INDEXCREATE
                {
                    szIndexName = "szIndexName",
                    cbKey = by_id.Length,
                    szKey = by_id,
                    grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                }
                }, 1);

                Api.JetCreateIndex2(session, tableid, new[]
                {
                new JET_INDEXCREATE
                {
                    szIndexName = "by_name_and_etag",
                    cbKey = by_name_and_etag.Length,
                    cbKeyMost = SystemParameters.KeyMost,
                    cbVarSegMac = SystemParameters.KeyMost,
                    szKey = by_name_and_etag,
                    grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                }
                }, 1);

                Api.JetCreateIndex2(session, tableid, new[]
                    {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_name_and_key",
                        cbKey = by_name_and_key.Length,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = by_name_and_key,
                        grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                    }
                }, 1);

                Api.JetCreateIndex2(session, tableid, new[]
                    {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_name_and_created_at",
                        cbKey = by_name_and_created_at.Length,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = by_name_and_created_at,
                        grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                    }
                }, 1);

            }

            using (var tbl = new Table(session, dbid, "indexed_documents_references", OpenTableGrbit.None))
            {
                var by_id = "+id\0\0";
                var by_key = "+key\0\0";
                var by_view_and_key = "+view\0+key\0\0";
                var by_ref = "+ref\0\0";

                var tableid = tbl.JetTableid;

                Api.JetDeleteIndex(session, tbl, "by_id");
                Api.JetDeleteIndex(session, tbl, "by_key");
                Api.JetDeleteIndex(session, tbl, "by_view_and_key");
                Api.JetDeleteIndex(session, tbl, "by_ref");

                Api.JetCreateIndex2(session, tableid, new[]
{
                new JET_INDEXCREATE
                {
                    szIndexName = "by_id",
                    cbKey = by_id.Length,
                    szKey = by_id,
                    grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                }
            }, 1);

                Api.JetCreateIndex2(session, tableid, new[]
                    {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_key",
                        cbKey = by_key.Length,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = by_key,
                        grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                    }
                }, 1);

                Api.JetCreateIndex2(session, tableid, new[]
                    {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_view_and_key",
                        cbKey = by_view_and_key.Length,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = by_view_and_key,
                        grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                    }
                }, 1);
                Api.JetCreateIndex2(session, tableid, new[]
                    {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_ref",
                        cbKey = by_ref.Length,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = by_ref,
                        grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                    }
                }, 1);
            }

            SchemaCreator.UpdateVersion(session, dbid, "5.5");
        }
    }
}
