//-----------------------------------------------------------------------
// <copyright file="TableColumnsCache.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent
{
	public class TableColumnsCache
	{
		public IDictionary<string, JET_COLUMNID> DocumentsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> TasksColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> FilesColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> IndexesStatsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> IndexesStatsReduceColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> IndexesEtagsColumns { get; set; }
		
		public IDictionary<string, JET_COLUMNID> MappedResultsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> ReduceResultsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> ScheduledReductionColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> DocumentsModifiedByTransactionsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> TransactionsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> IdentityColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> DetailsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> QueueColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> ListsColumns { get; set; }

		public IDictionary<string, JET_COLUMNID> ReduceKeysColumns { get; set; }

	    public void InitColumDictionaries(JET_INSTANCE instance, string database)
	    {
	        using (var session = new Session(instance))
	        {
	            var dbid = JET_DBID.Nil;
	            try
	            {
	                Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
	                using (var documents = new Table(session, dbid, "documents", OpenTableGrbit.None))
	                    DocumentsColumns = Api.GetColumnDictionary(session, documents);
					using (var lists = new Table(session, dbid, "lists", OpenTableGrbit.None))
						ListsColumns = Api.GetColumnDictionary(session, lists);
					using (var tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None))
	                    TasksColumns = Api.GetColumnDictionary(session, tasks);
					using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
						FilesColumns = Api.GetColumnDictionary(session, files);
					using (var scheduledReductions = new Table(session, dbid, "scheduled_reductions", OpenTableGrbit.None))
						ScheduledReductionColumns = Api.GetColumnDictionary(session, scheduledReductions);
					using (var indexStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None))
	                    IndexesStatsColumns = Api.GetColumnDictionary(session, indexStats);
					using (var indexStatsReduce = new Table(session, dbid, "indexes_stats_reduce", OpenTableGrbit.None))
						IndexesStatsReduceColumns = Api.GetColumnDictionary(session, indexStatsReduce);
					using (var indexEtags = new Table(session, dbid, "indexes_etag", OpenTableGrbit.None))
						IndexesEtagsColumns = Api.GetColumnDictionary(session, indexEtags);
					using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
	                    MappedResultsColumns = Api.GetColumnDictionary(session, mappedResults);
					using (var reduceResults = new Table(session, dbid, "reduce_results", OpenTableGrbit.None))
						ReduceResultsColumns = Api.GetColumnDictionary(session, reduceResults);
					using (
	                    var documentsModifiedByTransactions = new Table(session, dbid, "documents_modified_by_transaction",
	                                                                    OpenTableGrbit.None))
	                    DocumentsModifiedByTransactionsColumns = Api.GetColumnDictionary(session,
	                                                                                                       documentsModifiedByTransactions);
	                using (var transactions = new Table(session, dbid, "transactions", OpenTableGrbit.None))
	                    TransactionsColumns = Api.GetColumnDictionary(session, transactions);
	                using (var identity = new Table(session, dbid, "identity_table", OpenTableGrbit.None))
	                    IdentityColumns = Api.GetColumnDictionary(session, identity);
	                using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
	                    DetailsColumns = Api.GetColumnDictionary(session, details);
	                using (var queue = new Table(session, dbid, "queue", OpenTableGrbit.None))
	                    QueueColumns = Api.GetColumnDictionary(session, queue);
					using (var reduceKeys = new Table(session, dbid, "reduce_keys", OpenTableGrbit.None))
						ReduceKeysColumns = Api.GetColumnDictionary(session, reduceKeys);
	            }
	            finally
	            {
	                if (Equals(dbid, JET_DBID.Nil) == false)
	                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
	            }
	        }
	    }
	}
}
