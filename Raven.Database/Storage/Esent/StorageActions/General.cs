//-----------------------------------------------------------------------
// <copyright file="General.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	[CLSCompliant(false)]
	public partial class DocumentStorageActions : IDisposable, IGeneralStorageActions
	{
		public event Action OnCommit = delegate { }; 
		private readonly TableColumnsCache tableColumnsCache;
		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
	    private readonly IUuidGenerator uuidGenerator;
		private readonly IDocumentCacher cacher;
		private readonly TransactionalStorage transactionalStorage;
		protected readonly JET_DBID dbid;

		protected static readonly Logger logger = LogManager.GetCurrentClassLogger();
		protected readonly Session session;
		private readonly Transaction transaction;

		public JET_DBID Dbid
		{
			get { return dbid; }
		}

		public Session Session
		{
			get { return session; }
		}

		[CLSCompliant(false)]
		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public DocumentStorageActions(
			JET_INSTANCE instance, 
			string database, 
			TableColumnsCache tableColumnsCache, 
			OrderedPartCollection<AbstractDocumentCodec> documentCodecs, 
			IUuidGenerator uuidGenerator, 
			IDocumentCacher cacher, 
			TransactionalStorage transactionalStorage)
		{
			this.tableColumnsCache = tableColumnsCache;
			this.documentCodecs = documentCodecs;
		    this.uuidGenerator = uuidGenerator;
			this.cacher = cacher;
			this.transactionalStorage = transactionalStorage;
			try
			{
				session = new Session(instance);
				transaction = new Transaction(session);
				Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void Dispose()
		{
			if(queue != null)
				queue.Dispose();

			if(directories != null)
				directories.Dispose();

			if (details != null)
				details.Dispose();

			if (identity != null)
				identity.Dispose();

			if (transactions != null)
				transactions.Dispose();

			if (documentsModifiedByTransactions != null)
				documentsModifiedByTransactions.Dispose();

			if (mappedResults != null)
				mappedResults.Dispose();

			if (indexesStats != null)
				indexesStats.Dispose();

			if (files != null)
				files.Dispose();

			if (documents != null)
				documents.Dispose();

			if (tasks != null)
				tasks.Dispose();

			if (Equals(dbid, JET_DBID.Nil) == false && session != null)
				Api.JetCloseDatabase(session.JetSesid, dbid, CloseDatabaseGrbit.None);

			if (transaction != null)
				transaction.Dispose();

			if (session != null)
				session.Dispose();

		}

		public void Commit(CommitTransactionGrbit txMode)
		{
			transaction.Commit(txMode);
			OnCommit();
		}

		public long GetNextIdentityValue(string name)
		{
			Api.JetSetCurrentIndex(session, Identity, "by_key");
			Api.MakeKey(session, Identity, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Identity, SeekGrbit.SeekEQ) == false)
			{
				using (var update = new Update(session, Identity, JET_prep.Insert))
				{
					Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["key"], name, Encoding.Unicode);
					Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["val"], 1);

					update.Save();
				}
				return 1;
			}

			return Api.EscrowUpdate(session, Identity, tableColumnsCache.IdentityColumns["val"], 1) + 1;
		}


		public void RollbackTransaction(Guid txId)
		{
			CompleteTransaction(txId, doc =>
			{
				Api.JetSetCurrentIndex(session,Documents,"by_key");
				Api.MakeKey(session, Documents, doc.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ))
				{
					ResetTransactionOnCurrentDocument();
				}
			});
		}

		public void ModifyTransactionId(Guid fromTxId, Guid toTxId, TimeSpan timeout)
		{
			var transactionInformation = new TransactionInformation
			{
				Id = toTxId,
				Timeout = timeout
			};
			EnsureTransactionExists(transactionInformation);
			CompleteTransaction(fromTxId, doc =>
			{
				Api.JetSetCurrentIndex(session, Documents, "by_key");
				Api.MakeKey(session, Documents, doc.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ))
				{
					using (var update = new Update(session, Documents, JET_prep.Replace))
					{
						Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], toTxId);
						update.Save();
					}
				}

				if (doc.Delete)
					DeleteDocumentInTransaction(transactionInformation, doc.Key, null);
				else
					AddDocumentInTransaction(doc.Key, null, doc.Data, doc.Metadata, transactionInformation);
			});
		}
		
		public IEnumerable<Guid> GetTransactionIds()
		{
			Api.MoveBeforeFirst(session, Transactions);
			while(Api.TryMoveNext(session, Transactions))
			{
				var guid = Api.RetrieveColumnAsGuid(session, Transactions, tableColumnsCache.TransactionsColumns["tx_id"]);
				yield return (Guid)guid;
			}
		}

		public bool TransactionExists(Guid txId)
		{
			Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			return Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ);
		}

		public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
		{
			Api.JetSetCurrentIndex(session, Transactions, "by_tx_id");
			Api.MakeKey(session, Transactions, txId, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Transactions, SeekGrbit.SeekEQ))
				Api.JetDelete(session, Transactions);

			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_tx");
			Api.MakeKey(session, DocumentsModifiedByTransactions, txId, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ) == false)
				return;
			Api.MakeKey(session, DocumentsModifiedByTransactions, txId, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, DocumentsModifiedByTransactions, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			var bookmarksToDelete = new List<byte[]>();
			var documentsInTransaction = new List<DocumentInTransactionData>();
			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				if (
					Api.RetrieveColumnAsGuid(session, DocumentsModifiedByTransactions,
					                         tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"]) != txId)
					continue;
				var metadata = Api.RetrieveColumn(session, DocumentsModifiedByTransactions,
				                                  tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"]);
				var key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions,
				                                     tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"],
				                                     Encoding.Unicode);

				RavenJObject dataAsJson;
				var metadataAsJson = metadata.ToJObject();
				using (
					Stream stream = new BufferedStream(new ColumnStream(session, DocumentsModifiedByTransactions,
													 tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"])))
				{
					using (var data = documentCodecs.Aggregate(stream, (dataStream, codec) => codec.Decode(key, metadataAsJson, dataStream)))
						dataAsJson = data.ToJObject();
				}


				documentsInTransaction.Add(new DocumentInTransactionData
				{
					Data = dataAsJson,
					Delete =
						Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions,
						                            tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"]).Value,
					Etag = Api.RetrieveColumn(session, DocumentsModifiedByTransactions,
					                          tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"]).
						TransfromToGuidWithProperSorting(),
					Key = key,
					Metadata = metadata.ToJObject(),
				});
				bookmarksToDelete.Add(Api.GetBookmark(session, DocumentsModifiedByTransactions));
			} while (Api.TryMoveNext(session, DocumentsModifiedByTransactions));

			foreach (var bookmark in bookmarksToDelete)
			{
				Api.JetGotoBookmark(session, DocumentsModifiedByTransactions, bookmark, bookmark.Length);
				Api.JetDelete(session, DocumentsModifiedByTransactions);
			}
			foreach (var documentInTransactionData in documentsInTransaction)
			{
				perDocumentModified(documentInTransactionData);
			}
		}

		private void ResetTransactionOnCurrentDocument()
		{
			using (var update = new Update(session, Documents, JET_prep.Replace))
			{
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], null);
				update.Save();
			}
		}


	}

	
}
