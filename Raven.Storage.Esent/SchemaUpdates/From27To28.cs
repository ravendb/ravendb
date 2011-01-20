//-----------------------------------------------------------------------
// <copyright file="From27To28.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Extensions;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From27To28 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "2.7"; }
		}

	    private IUuidGenerator uuidGenerator;

	    public void Init(IUuidGenerator generator)
	    {
	        uuidGenerator = generator;
	    }

	    public void Update(Session session, JET_DBID dbid)
		{
			Transaction tx;
			using (tx = new Transaction(session))
			{
				int count = 0;
				const int rowsInTxCount = 100;
				using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
				{
					var columnid = Api.GetColumnDictionary(session, files)["etag"];
					Api.MoveBeforeFirst(session, files);
					while(Api.TryMoveNext(session, files))
					{
						using(var update = new Update(session, files, JET_prep.Replace))
						{
							Api.SetColumn(session, files, columnid, uuidGenerator.CreateSequentialUuid().TransformToValueForEsentSorting());
							update.Save();
						}
						if(count++ % rowsInTxCount == 0)
						{
							tx.Commit(CommitTransactionGrbit.LazyFlush);
							tx.Dispose();
							tx = new Transaction(session);
						}
					}
					tx.Commit(CommitTransactionGrbit.LazyFlush);
					tx.Dispose();
					tx = new Transaction(session);
				}

				using (var documents = new Table(session, dbid, "documents", OpenTableGrbit.None))
				{
					var columnid = Api.GetColumnDictionary(session, documents)["etag"];
					Api.MoveBeforeFirst(session, documents);
					while (Api.TryMoveNext(session, documents))
					{
						using (var update = new Update(session, documents, JET_prep.Replace))
						{
							Api.SetColumn(session, documents, columnid, uuidGenerator.CreateSequentialUuid().TransformToValueForEsentSorting());
							update.Save();
						}
						if (count++ % rowsInTxCount == 0)
						{
							tx.Commit(CommitTransactionGrbit.LazyFlush);
							tx.Dispose();
							tx = new Transaction(session);
						}
					}
					tx.Commit(CommitTransactionGrbit.LazyFlush);
					tx.Dispose();
					tx = new Transaction(session);
				}

				using (var indexesStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None))
				{
					var columnid = Api.GetColumnDictionary(session, indexesStats)["last_indexed_etag"];
					Api.MoveBeforeFirst(session, indexesStats);
					while (Api.TryMoveNext(session, indexesStats))
					{
						using (var update = new Update(session, indexesStats, JET_prep.Replace))
						{
							Api.SetColumn(session, indexesStats, columnid, Guid.Empty.TransformToValueForEsentSorting());
							update.Save();
						}
						if (count++ % rowsInTxCount == 0)
						{
							tx.Commit(CommitTransactionGrbit.LazyFlush);
							tx.Dispose();
							tx = new Transaction(session);
						}
					}
					tx.Commit(CommitTransactionGrbit.LazyFlush);
					tx.Dispose();
					tx = new Transaction(session);
				}

				using (var documentsModifiedByTransaction = new Table(session, dbid, "documents_modified_by_transaction", OpenTableGrbit.None))
				{
					var columnid = Api.GetColumnDictionary(session, documentsModifiedByTransaction)["etag"];
					Api.MoveBeforeFirst(session, documentsModifiedByTransaction);
					while (Api.TryMoveNext(session, documentsModifiedByTransaction))
					{
						using (var update = new Update(session, documentsModifiedByTransaction, JET_prep.Replace))
						{
							Api.SetColumn(session, documentsModifiedByTransaction, columnid, uuidGenerator.CreateSequentialUuid().TransformToValueForEsentSorting());
							update.Save();
						}
						if (count++ % rowsInTxCount == 0)
						{
							tx.Commit(CommitTransactionGrbit.LazyFlush);
							tx.Dispose();
							tx = new Transaction(session);
						}
					}
					tx.Commit(CommitTransactionGrbit.LazyFlush);
					tx.Dispose();
					tx = new Transaction(session);
				
				}

				using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
				{
					var columnid = Api.GetColumnDictionary(session, mappedResults)["etag"];
					Api.MoveBeforeFirst(session, mappedResults);
					while (Api.TryMoveNext(session, mappedResults))
					{
						using (var update = new Update(session, mappedResults, JET_prep.Replace))
						{
							Api.SetColumn(session, mappedResults, columnid, uuidGenerator.CreateSequentialUuid().TransformToValueForEsentSorting());
							update.Save();
						} 
						if (count++ % rowsInTxCount == 0)
						{
							tx.Commit(CommitTransactionGrbit.LazyFlush);
							tx.Dispose();
							tx = new Transaction(session);
						}
					}
					tx.Commit(CommitTransactionGrbit.LazyFlush);
					tx.Dispose();
					tx = new Transaction(session);
			
				}

				using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
				{
					Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
					var columnids = Api.GetColumnDictionary(session, details);

					using (var update = new Update(session, details, JET_prep.Replace))
					{
						Api.SetColumn(session, details, columnids["schema_version"], "2.8", Encoding.Unicode);

						update.Save();
					}
				}
				tx.Commit(CommitTransactionGrbit.None);
			}
		}
	}
}
