// -----------------------------------------------------------------------
//  <copyright file="EsentInFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Mono.Cecil;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Database.Server;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Storage.Esent;

namespace Raven.Database.Impl.DTC
{
	public class EsentInFlightTransactionalState : InFlightTransactionalState, IDisposable
	{
		private readonly TransactionalStorage storage;
		private readonly DocumentDatabase docDb;
		private readonly CommitTransactionGrbit txMode;
		private readonly ConcurrentDictionary<string, EsentTransactionContext> transactionContexts =
			new ConcurrentDictionary<string, EsentTransactionContext>();

		private long transactionContextNumber;
		private readonly Timer timer;

		public EsentInFlightTransactionalState(DocumentDatabase docDb,
			TransactionalStorage storage,
			CommitTransactionGrbit txMode, 
			Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, 
			Func<string, Etag, TransactionInformation, bool> databaseDelete)
			: base(databasePut, databaseDelete)
		{
			this.storage = storage;
			this.docDb = docDb;
			this.txMode = txMode;
			timer = new Timer(CleanupOldTransactions, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
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
			using (LogContext.WithDatabase(docDb.Name ?? Constants.SystemDatabase))
			{
				var oldestAllowedTransaction = SystemTime.UtcNow;
				log.Info("Performing Transactions Cleanup Sequence for db {0}", docDb.Name ?? Constants.SystemDatabase);
				foreach (var ctx in transactionContexts.ToArray())
				{
					var age = oldestAllowedTransaction - ctx.Value.CreatedAt;
					if (age.TotalMinutes >= 3)
					{
						log.Info("Rolling back DTC transaction {0} because it is too old {1}", ctx.Key, age);
						try
						{
							Rollback(ctx.Key);
						}
						catch (Exception e)
						{
							log.WarnException("Could not properly rollback transaction", e);
						}
					}
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

					if (context.DocumentIdsToTouch != null)
					{
						using (docDb.DocumentLock.Lock())
						{
							using (storage.DisableBatchNesting())
							{
								storage.Batch(accessor =>
								{
									foreach (var docId in context.DocumentIdsToTouch)
									{
										docDb.CheckReferenceBecauseOfDocumentUpdate(docId, accessor);
										try
										{
											Etag preTouchEtag;
											Etag afterTouchEtag;
											accessor.Documents.TouchDocument(docId, out preTouchEtag, out afterTouchEtag);
										}
										catch (ConcurrencyException)
										{
                                            log.Info("Concurrency exception when touching {0}", docId);
               
										}
									}
								});
							}
						}
					}
					
					foreach (var afterCommit in context.ActionsAfterCommit)
					{
						afterCommit();
					}
				}
			}
		}

		public override void Prepare(string id, Guid? resourceManagerId, byte[] recoveryInformation)
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
			    List<DocumentInTransactionData> changes = null;
                using (storage.SetTransactionContext(context))
				{
					storage.Batch(accessor =>
					{
					    var documentsToTouch = RunOperationsInTransaction(id, out changes);
					    context.DocumentIdsToTouch = documentsToTouch;
					});
				}

			    if (changes == null) 
                    return;

			    // independent storage transaction, will actually commit here
			    storage.Batch(accessor =>
			    {
			        var data = new RavenJObject
			        {
			            {"Changes", RavenJToken.FromObject(changes, new JsonSerializer
			            {
			                Converters =
			                {
			                    new JsonToJsonConverter(),
                                new EtagJsonConverter()
			                }
			            })
			            },
			            {"ResourceManagerId", resourceManagerId.ToString()},
			            {"RecoveryInformation", recoveryInformation}
			        };
			        accessor.Lists.Set("Raven/Transactions/Pending", id, data,
			            UuidType.DocumentTransactions);
			    });
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

			var lockTaken = false;
			Monitor.Enter(context, ref lockTaken);
			try
			{
                storage.Batch(accessor => accessor.Lists.Remove("Raven/Transactions/Pending", id));

				context.Dispose();
			}
			finally
			{
				if (lockTaken)
					Monitor.Exit(context);
			}
		}

        internal List<TransactionContextData> GetTransactionContextsData()
	    {
            var results = new List<TransactionContextData>();

            try
            {

            
            foreach (var transactionName in transactionContexts.Keys)
            {
                EsentTransactionContext curContext;
                if (transactionContexts.TryGetValue(transactionName, out curContext))
                {
                    results.Add(new TransactionContextData(){
                        
                        Id = transactionName,
                    CreatedAt = curContext.CreatedAt,
                        DocumentIdsToTouch = curContext.DocumentIdsToTouch != null? curContext.DocumentIdsToTouch.ToList():null,
                    IsAlreadyInContext = curContext.AlreadyInContext,
                        NumberOfActionsAfterCommit = curContext.ActionsAfterCommit!= null?curContext.ActionsAfterCommit.Count:0
                        });
                }
            }

            }
            catch (Exception ex)
            {

                throw;
            }
	        return  results;

            
            
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