// -----------------------------------------------------------------------
//  <copyright file="From46To47.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From46To47 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.6"; } }

		public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			Api.JetDeleteTable(session, dbid, "transactions");
			Api.JetDeleteTable(session, dbid, "documents_modified_by_transaction");

			using (var sr = new Table(session, dbid, "scheduled_reductions", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, sr, "by_view_level_and_hashed_reduce_key");
				Api.JetDeleteIndex(session, sr, "by_view_level_bucket_and_hashed_reduce_key");

				SchemaCreator.CreateIndexes(session, sr,
				                            new JET_INDEXCREATE
				                            {
					                            szIndexName = "by_view_level_and_hashed_reduce_key_and_bucket",
					                            szKey = "+view\0+level\0+hashed_reduce_key\0+bucket\0\0",
				                            });
			}

			SchemaCreator.UpdateVersion(session, dbid, "4.7");
		} 
	}
}