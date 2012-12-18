//-----------------------------------------------------------------------
// <copyright file="TableProperties.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions
	{
		private Table documents;
		protected Table Documents
		{
			get { return documents ?? (documents = new Table(session, dbid, "documents", OpenTableGrbit.None)); }
		}

		private Table queue;
		protected Table Queue
		{
			get { return queue ?? (queue = new Table(session, dbid, "queue", OpenTableGrbit.None)); }
		}

		private Table lists;
		protected Table Lists
		{
			get { return lists ?? (lists = new Table(session, dbid, "lists", OpenTableGrbit.None)); }
		}

		private Table transactions;
		protected Table Transactions
		{
			get { return transactions ?? (transactions = new Table(session, dbid, "transactions", OpenTableGrbit.None)); }
		}

		private Table directories;
		protected internal Table Directories
		{
			get { return directories ?? (directories = new Table(session, dbid, "directories", OpenTableGrbit.None)); }
		}

		private Table documentsModifiedByTransactions;
		protected Table DocumentsModifiedByTransactions
		{
			get
			{
				return documentsModifiedByTransactions ??
					(documentsModifiedByTransactions =
						new Table(session, dbid, "documents_modified_by_transaction", OpenTableGrbit.None));
			}
		}

		private Table files;
		protected Table Files
		{
			get { return files ?? (files = new Table(session, dbid, "files", OpenTableGrbit.None)); }
		}

		private Table indexesStats;
		protected Table IndexesStats
		{
			get { return indexesStats ?? (indexesStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None)); }
		}

		private Table indexesStatsReduce;
		protected Table IndexesStatsReduce
		{
			get { return indexesStatsReduce ?? (indexesStatsReduce = new Table(session, dbid, "indexes_stats_reduce", OpenTableGrbit.None)); }
		}

		private Table indexesEtags;
		protected Table IndexesEtags
		{
			get { return indexesEtags ?? (indexesEtags = new Table(session, dbid, "indexes_etag", OpenTableGrbit.None)); }
		}


		private Table scheduledReductions;
		protected Table ScheduledReductions
		{
			get { return scheduledReductions ?? (scheduledReductions = new Table(session, dbid, "scheduled_reductions", OpenTableGrbit.None)); }
		}

		private Table mappedResults;
		protected Table MappedResults
		{
			get { return mappedResults ?? (mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None)); }
		}

		private Table reducedResults;
		protected Table ReducedResults
		{
			get { return reducedResults ?? (reducedResults = new Table(session, dbid, "reduce_results", OpenTableGrbit.None)); }
		}

		private Table tasks;

		protected Table Tasks
		{
			get { return tasks ?? (tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None)); }
		}

		private Table identity;
		protected Table Identity
		{
			get { return identity ?? (identity = new Table(session, dbid, "identity_table", OpenTableGrbit.None)); }
		}

		private Table details;
		protected Table Details
		{
			get { return details ?? (details = new Table(session, dbid, "details", OpenTableGrbit.None)); }
		}

		private Table reduceKeys;
		protected Table ReduceKeys
		{
			get { return reduceKeys ?? (reduceKeys = new Table(session, dbid, "reduce_keys", OpenTableGrbit.None)); }
		}
	}
}
