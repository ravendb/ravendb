// -----------------------------------------------------------------------
//  <copyright file="EsentInFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Esent;

namespace Raven.Database.Impl.DTC
{
	public class EsentInFlightTransactionalState : InFlightTransactionalState, IDisposable
	{
		private readonly TransactionalStorage storage;
		private readonly CommitTransactionGrbit txMode;
		private readonly ConcurrentDictionary<string, EsentTransactionContext> transactionContexts =
			new ConcurrentDictionary<string, EsentTransactionContext>();

		private long transactionContextNumber = 0;
		private readonly Func<EsentTransactionContext> createContext;

		public EsentInFlightTransactionalState(TransactionalStorage storage, CommitTransactionGrbit txMode, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, Func<string, Etag, TransactionInformation, bool> databaseDelete)
			: base(databasePut, databaseDelete)
		{
			this.storage = storage;
			this.txMode = txMode;
			createContext = () =>
			{
				var newTransactionNumber = Interlocked.Increment(ref transactionContextNumber);
				return new EsentTransactionContext(new Session(storage.Instance),
												   new IntPtr(newTransactionNumber));
			};
		}

		public override void Commit(string id)
		{
			EsentTransactionContext context;
			if (transactionContexts.TryGetValue(id, out context) == false)
				throw new InvalidOperationException("There is no transaction with id: " + id + " ready to commit. Did you call PrepareTransaction?");

			lock (context)
			{
				using (context.EnterSessionContext())
				{
					context.Transaction.Commit(txMode);

					foreach (var afterCommit in context.ActionsAfterCommit)
					{
						afterCommit();
					}
				}
			}
		}

		public override void Prepare(string id)
		{
			var context = transactionContexts.GetOrAdd(id, createContext());

			using (storage.SetTransactionContext(context))
			{
				storage.Batch(accessor => RunOperationsInTransaction(id));
			}
		}

		public override void Rollback(string id)
		{
			base.Rollback(id);

			EsentTransactionContext context;
			if (transactionContexts.TryRemove(id, out context) == false)
				return;

			using (context.EnterSessionContext())
			{
				context.Transaction.Dispose(); // will rollback the transaction if it was not committed
			}
			context.Session.Dispose();
		}

		public void Dispose()
		{
			foreach (var context in transactionContexts)
			{
				using (context.Value.EnterSessionContext())
				{
					context.Value.Transaction.Dispose();
				}
				context.Value.Session.Dispose();
			}
		}
	}
}