// -----------------------------------------------------------------------
//  <copyright file="From47To48.cs" company="Hibernating Rhinos LTD">
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
	public class From47To48 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.7"; } }

		public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			using (var tbl = new Table(session, dbid, "documents", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, tbl, "by_key");
				Api.JetCreateIndex2(session, tbl, new[]
                {
                    new JET_INDEXCREATE
                    {
                        szIndexName = "by_key",
                        cbKey = 6,
                        cbKeyMost = SystemParameters.KeyMost,
                        cbVarSegMac = SystemParameters.KeyMost,
                        szKey = "+key\0\0",
                        grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
                    }
                }, 1);
			}
			SchemaCreator.UpdateVersion(session, dbid, "4.8");
		}
	}
}