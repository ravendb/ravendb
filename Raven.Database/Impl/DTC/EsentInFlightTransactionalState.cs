// -----------------------------------------------------------------------
//  <copyright file="EsentInFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
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

		private long transactionContextNumber;
		private readonly Timer timer;

		public EsentInFlightTransactionalState(TransactionalStorage storage, CommitTransactionGrbit txMode, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, Func<string, Etag, TransactionInformation, bool> databaseDelete)
			: base(databasePut, databaseDelete)
		{
			this.storage = storage;
			this.txMode = txMode;
			timer = new Timer(CleanupOldTransactions, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
		}

	    public EsentTransactionContext CreateEsentTransactionContext()
		{
			var newTransactionNumber = Interlocked.Increment(ref transactionContextNumber);
			return new EsentTransactionContext(new Session(storage.Instance),
			                                   new IntPtr(newTransactionNumber),
			                                   SystemTime.UtcNow);
		}

		private void CleanupOldTransactions(object state)
		{
			var oldestAllowedTransaction = SystemTime.UtcNow;
			foreach (var ctx in transactionContexts.ToArray())
			{
				var age = oldestAllowedTransaction - ctx.Value.CreatedAt;
				if (age.TotalMinutes >= 5)
				{
					log.Info("Rolling back DTC transaction {0} because it is too old {1}", ctx.Key, age);
					Rollback(ctx.Key);
				}
			}
		}

		public override void Commit(string id)
		{
			EsentTransactionContext context;
			if (transactionContexts.TryGetValue(id, out context) == false)
				throw new InvalidOperationException("There is no transaction with id: " + id + " ready to commit. Did you call PrepareTransaction?");

			lock (context)
			{
				//using(context.Session) - disposing the session is actually done in the rollback, which is always called
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
			EsentTransactionContext context;
			if (transactionContexts.TryGetValue(id, out context) == false)
			{
				var myContext = CreateEsentTransactionContext();
				try
				{
					context = transactionContexts.GetOrAdd(id, myContext);
				}
				finally
				{
					if (myContext != context)
						myContext.Dispose();
				}
			}
			try
			{
				using (storage.SetTransactionContext(context))
				{
					storage.Batch(accessor => RunOperationsInTransaction(id));
				}
			}
			catch (Exception)
			{
				Rollback(id);
				throw;
			}
		}

		public override void Rollback(string id)
		{
			base.Rollback(id);

			EsentTransactionContext context;
			if (transactionContexts.TryRemove(id, out context) == false)
				return;

			context.Dispose();
		}

		public void Dispose()
		{
			timer.Dispose();
			foreach (var context in transactionContexts)
			{
				using (context.Value.Session)
				using (context.Value.EnterSessionContext())
				{
					context.Value.Transaction.Dispose();
				}
			}
		}
	}
}