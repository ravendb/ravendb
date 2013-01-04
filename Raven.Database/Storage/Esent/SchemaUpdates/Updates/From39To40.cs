// -----------------------------------------------------------------------
//  <copyright file="From37To38.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From39To40 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "3.9"; } }

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			JET_COLUMNID columnid;

			using (var scheduledReductions = new Table(session, dbid, "scheduled_reductions", OpenTableGrbit.None))
			{
				Api.JetAddColumn(session, scheduledReductions, "hashed_reduce_key", new JET_COLUMNDEF
				{
					coltyp = JET_coltyp.Binary,
					cbMax = 20,
					grbit = ColumndefGrbit.ColumnFixed
				}, null, 0, out columnid);

				var scheduledReductionsColumns = Api.GetColumnDictionary(session, scheduledReductions);
				Api.MoveBeforeFirst(session, scheduledReductions);
				while (Api.TryMoveNext(session, scheduledReductions))
				{
					using (var update = new Update(session, scheduledReductions, JET_prep.Replace))
					{
						var reduceKey = Api.RetrieveColumnAsString(session, scheduledReductions, scheduledReductionsColumns["reduce_key"]);

						Api.SetColumn(session, scheduledReductions, scheduledReductionsColumns["hashed_reduce_key"],
						              DocumentStorageActions.HashReduceKey(reduceKey));

						update.Save();
					}
				}

				Api.JetDeleteIndex(session, scheduledReductions, "by_view_level_reduce_key_and_bucket");
				SchemaCreator.CreateIndexes(session, scheduledReductions, new JET_INDEXCREATE
				{
					szIndexName = "by_view_level_bucket_and_hashed_reduce_key",
					szKey = "+view\0+level\0+bucket\0+hashed_reduce_key\0\0",
				});
			}

			using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
			{
				Api.JetAddColumn(session, mappedResults, "hashed_reduce_key", new JET_COLUMNDEF
				{
					coltyp = JET_coltyp.Binary,
					cbMax = 20,
					grbit = ColumndefGrbit.ColumnFixed
				}, null, 0, out columnid);

				var mappedResultsColumns = Api.GetColumnDictionary(session, mappedResults);
				Api.MoveBeforeFirst(session, mappedResults);
				while (Api.TryMoveNext(session, mappedResults))
				{
					using (var update = new Update(session, mappedResults, JET_prep.Replace))
					{
						var reduceKey = Api.RetrieveColumnAsString(session, mappedResults, mappedResultsColumns["reduce_key"]);

						Api.SetColumn(session, mappedResults, mappedResultsColumns["hashed_reduce_key"], DocumentStorageActions.HashReduceKey(reduceKey));

						update.Save();
					}
				}

				Api.JetDeleteIndex(session, mappedResults, "by_view_reduce_key_and_bucket");
				SchemaCreator.CreateIndexes(session, mappedResults, new JET_INDEXCREATE
				{
					szIndexName = "by_view_bucket_and_hashed_reduce_key",
					szKey = "+view\0+bucket\0+hashed_reduce_key\0\0",
				});
			}

			using (var reduceResults = new Table(session, dbid, "reduce_results", OpenTableGrbit.None))
			{
				Api.JetAddColumn(session, reduceResults, "hashed_reduce_key", new JET_COLUMNDEF
				{
					coltyp = JET_coltyp.Binary,
					cbMax = 20,
					grbit = ColumndefGrbit.ColumnFixed
				}, null, 0, out columnid);

				var reduceResultsColumns = Api.GetColumnDictionary(session, reduceResults);
				Api.MoveBeforeFirst(session, reduceResults);
				while (Api.TryMoveNext(session, reduceResults))
				{
					using (var update = new Update(session, reduceResults, JET_prep.Replace))
					{
						var reduceKey = Api.RetrieveColumnAsString(session, reduceResults, reduceResultsColumns["reduce_key"]);

						Api.SetColumn(session, reduceResults, reduceResultsColumns["hashed_reduce_key"], DocumentStorageActions.HashReduceKey(reduceKey));

						update.Save();
					}
				}

				Api.JetDeleteIndex(session, reduceResults, "by_view_level_reduce_key_and_bucket");
				Api.JetDeleteIndex(session, reduceResults, "by_view_level_reduce_key_and_source_bucket");
				SchemaCreator.CreateIndexes(session, reduceResults,
				                            new JET_INDEXCREATE
				                            {
					                            szIndexName = "by_view_level_bucket_and_hashed_reduce_key",
					                            szKey = "+view\0+level\0+bucket\0+hashed_reduce_key\0\0",
				                            },
				                            new JET_INDEXCREATE
				                            {
					                            szIndexName = "by_view_level_source_bucket_and_hashed_reduce_key",
					                            szKey = "+view\0+level\0+source_bucket\0+hashed_reduce_key\0\0",
				                            });
			}

			SchemaCreator.UpdateVersion(session, dbid, "4.0");
		}
	}
}