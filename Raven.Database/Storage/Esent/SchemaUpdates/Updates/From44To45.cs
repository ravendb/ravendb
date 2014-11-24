using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From44To45 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.4"; } }
		
		public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			using (var table = new Table(session, dbid, "scheduled_reductions", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, table, "by_view");
			}

			using (var table = new Table(session, dbid, "indexed_documents_references", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, table, "by_view");
			}

			using (var table = new Table(session, dbid, "reduce_keys_counts", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, table, "by_view");
			}

			using (var table = new Table(session, dbid, "reduce_keys_status", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, table, "by_view");
			}

			using (var table = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, table, "by_view");
				Api.JetDeleteIndex(session, table, "by_view_and_etag");
				Api.JetDeleteIndex(session, table, "by_view_bucket_and_hashed_reduce_key");
				Api.JetDeleteIndex(session, table, "by_view_and_hashed_reduce_key");

				SchemaCreator.CreateIndexes(session, table, new JET_INDEXCREATE
				{
					szIndexName = "by_view_hashed_reduce_key_and_bucket",
					szKey = "+view\0+hashed_reduce_key\0+bucket\0\0",
				});
			}

			using (var table = new Table(session, dbid, "reduce_results", OpenTableGrbit.None))
			{
				Api.JetDeleteIndex(session, table, "by_view");
				Api.JetDeleteIndex(session, table, "by_view_level_bucket_and_hashed_reduce_key");

				SchemaCreator.CreateIndexes(session, table, new JET_INDEXCREATE
				{
					szIndexName = "by_view_level_hashed_reduce_key_and_bucket",
					szKey = "+view\0+level\0+hashed_reduce_key\0+bucket\0\0",
				});
			}

			SchemaCreator.UpdateVersion(session, dbid, "4.5");
		}
	}
}