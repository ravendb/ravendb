// -----------------------------------------------------------------------
//  <copyright file="InFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Storage;
using Raven.Json.Linq;
using System.Linq;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;
using Raven.Abstractions.Threading;

namespace Raven.Database.Impl.DTC
{
	public abstract class InFlightTransactionalState
	{
		protected static readonly ILog log = LogManager.GetCurrentClassLogger();

		protected readonly Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> DatabasePut;
		protected readonly Func<string, Etag, TransactionInformation, bool> DatabaseDelete;

		protected class TransactionState
		{
			public readonly List<DocumentInTransactionData> Changes = new List<DocumentInTransactionData>();

			[CLSCompliant(false)]
			public volatile Reference<DateTime> LastSeen = new Reference<DateTime>();

			private TimeSpan timeout;
			public TimeSpan Timeout
			{
				get
				{
					if (timeout < TimeSpan.FromSeconds(30))
						return TimeSpan.FromSeconds(30);
					return timeout;
		}

				set { timeout = value; }
			}

			public TransactionState()
			{
				Timeout = TimeSpan.FromMinutes(3);
			}
		}

		protected class ChangedDoc
		{
			public string transactionId;
			public Etag currentEtag;
			public Etag committedEtag;
		}

		protected readonly ConcurrentDictionary<string, ChangedDoc> changedInTransaction = new ConcurrentDictionary<string, ChangedDoc>();

		protected readonly ConcurrentDictionary<string, TransactionState> transactionStates = new ConcurrentDictionary<string, TransactionState>();

		public object GetInFlightTransactionsInternalStateForDebugOnly()
		{
			return new { changedInTransaction, transactionStates };
		}

		protected InFlightTransactionalState(Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, Func<string, Etag, TransactionInformation, bool> databaseDelete)
		{
			this.DatabasePut = databasePut;
			this.DatabaseDelete = databaseDelete;
		}

		public Etag AddDocumentInTransaction(
			string key,
			Etag etag,
			RavenJObject data,
			RavenJObject metadata,
			TransactionInformation transactionInformation,
			Etag committedEtag,
			IUuidGenerator uuidGenerator)
		{
			metadata.EnsureCannotBeChangeAndEnableSnapshotting();
			data.EnsureCannotBeChangeAndEnableSnapshotting();
			return AddToTransactionState(key, etag,
								  transactionInformation,
								  committedEtag,
								  new DocumentInTransactionData
								  {
									  Metadata = metadata,
									  Data = data,
									  Delete = false,
									  Key = key,
									  LastModified = SystemTime.UtcNow,
									  Etag = uuidGenerator.CreateSequentialUuid(UuidType.DocumentTransactions)
								  });
		}

		public void DeleteDocumentInTransaction(
			TransactionInformation transactionInformation,
			string key,
			Etag etag,
			Etag committedEtag,
			IUuidGenerator uuidGenerator)
		{
			AddToTransactionState(key, etag, transactionInformation, committedEtag, new DocumentInTransactionData
			{
				Delete = true,
				Key = key,
				LastModified = SystemTime.UtcNow
			});
		}

		public bool IsModified(string key)
		{
			var value = currentlyCommittingTransaction.Value;
			if (string.IsNullOrEmpty(value))
				return changedInTransaction.ContainsKey(key);
			ChangedDoc doc;
			if (changedInTransaction.TryGetValue(key, out doc) == false)
				return false;
			return doc.transactionId != value;
		}

		public Func<TDocument, TDocument> GetNonAuthoritativeInformationBehavior<TDocument>(TransactionInformation tx, string key) where TDocument : class, IJsonDocumentMetadata, new()
		{
			ChangedDoc existing;
			if (changedInTransaction.TryGetValue(key, out existing) == false || (tx != null && tx.Id == existing.transactionId))
				return null;

			if (transactionStates.ContainsKey(existing.transactionId) == false)
			{
				changedInTransaction.TryRemove(key, out existing);
				return null;// shouldn't happen, but we have better be on the safe side
			}

			return document =>
			{
				if (document == null)
				{
					return new TDocument
					{
						Key = key,
						Metadata = new RavenJObject { { Constants.RavenDocumentDoesNotExists, true } },
						LastModified = DateTime.MinValue,
						NonAuthoritativeInformation = true,
						Etag = Etag.Empty
					};
				}

				document.NonAuthoritativeInformation = true;
				return document;
			};
		}

        public virtual void Rollback(string id)
		{
			TransactionState value;
			if (transactionStates.TryRemove(id, out value) == false)
				return;
			lock (value)
			{
				foreach (var change in value.Changes)
				{
					ChangedDoc guid;
					changedInTransaction.TryRemove(change.Key, out guid);
				}
				value.Changes.Clear();
			}
		}

        protected readonly Raven.Abstractions.Threading.ThreadLocal<string> currentlyCommittingTransaction = new Raven.Abstractions.Threading.ThreadLocal<string>();

		public abstract void Commit(string id);

		public abstract void Prepare(string id, Guid? resourceManagerId, byte[] recoveryInformation);

		private Etag AddToTransactionState(string key,
			Etag etag,
			TransactionInformation transactionInformation,
			Etag committedEtag,
			DocumentInTransactionData item)
		{
			try
			{
				var state = transactionStates.GetOrAdd(transactionInformation.Id, id => new TransactionState());
				lock (state)
				{
					state.LastSeen = new Reference<DateTime>
					{
						Value = SystemTime.UtcNow
					};
					state.Timeout = transactionInformation.Timeout;

					var currentTxVal = state.Changes.LastOrDefault(x => string.Equals(x.Key, key, StringComparison.InvariantCultureIgnoreCase));
					if (currentTxVal != null)
					{
						EnsureValidEtag(key, etag, committedEtag, currentTxVal);
						state.Changes.Remove(currentTxVal);
					}
				    var result = changedInTransaction.AddOrUpdate(key, s =>
					{
						EnsureValidEtag(key, etag, committedEtag, currentTxVal);

						return new ChangedDoc
						{
							transactionId = transactionInformation.Id,
							committedEtag = committedEtag,
							currentEtag = item.Etag
						};
					}, (_, existing) =>
					{
						if (existing.transactionId == transactionInformation.Id)
						{
							EnsureValidEtag(key, etag, committedEtag, currentTxVal);
							existing.currentEtag = item.Etag;
							return existing;
						}

						throw new ConcurrencyException("Document " + key + " is being modified by another transaction: " + existing);
					});

					state.Changes.Add(item);

					return result.currentEtag;
				}
			}
			catch (Exception)
			{
				Rollback(transactionInformation.Id);
				throw;
			}
		}

	    private static void EnsureValidEtag(string key, Etag etag, Etag committedEtag, DocumentInTransactionData currentTxVal)
	    {
	        if (etag == null)
	            return;
            if (currentTxVal != null && currentTxVal.Delete)
	        {
                if (etag != Etag.Empty)
	                throw new ConcurrencyException("Transaction operation attempted on : " + key + " using a non current etag (delete)");
	            return;
	        }

	        if (currentTxVal != null)
	        {
	            if (currentTxVal.Etag != etag)
	            {
	                throw new ConcurrencyException("Transaction operation attempted on : " + key +
	                                               " using a non current etag");
	            }
	            return;
	        }

			if (etag != committedEtag)
				throw new ConcurrencyException("Transaction operation attempted on : " + key + " using a non current etag");
	    }

	    public bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocument document)
		{
			return TryGetInternal(key, transactionInformation, (theKey, change) => new JsonDocument
			{
				DataAsJson = (RavenJObject)change.Data.CreateSnapshot(),
				Metadata = (RavenJObject)change.Metadata.CreateSnapshot(),
				Key = theKey,
				Etag = change.Etag,
				NonAuthoritativeInformation = false,
				LastModified = change.LastModified
			}, out document);
		}

		public bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocumentMetadata document)
		{
			return TryGetInternal(key, transactionInformation, (theKey, change) => new JsonDocumentMetadata
			{
				Metadata = (RavenJObject)change.Metadata.CreateSnapshot(),
				Key = theKey,
				Etag = change.Etag,
				NonAuthoritativeInformation = false,
				LastModified = change.LastModified
			}, out document);
		}

		private bool TryGetInternal<T>(string key, TransactionInformation transactionInformation, Func<string, DocumentInTransactionData, T> createDoc, out T document)
			where T : class
		{
			TransactionState state;
			if (transactionStates.TryGetValue(transactionInformation.Id, out state) == false)
			{
				document = null;
				return false;
			}
			var change = state.Changes.LastOrDefault(x => string.Equals(x.Key, key, StringComparison.InvariantCultureIgnoreCase));
			if (change == null)
			{
				document = null;
				return false;
			}
			if (change.Delete)
			{
				document = null;
				return true;
			}
			document = createDoc(key, change);
			return true;
		}

        public bool HasTransaction(string txId)
		{
			return transactionStates.ContainsKey(txId);
		}

        protected HashSet<string> RunOperationsInTransaction(string id, out List<DocumentInTransactionData> changes)
        {
            changes = null;
			TransactionState value;
		    if (transactionStates.TryGetValue(id, out value) == false)
		        return null; // no transaction, cannot do anything to this

			changes = value.Changes;
			lock (value)
			{
				value.LastSeen = new Reference<DateTime>
				{
					Value = SystemTime.UtcNow
				};
				currentlyCommittingTransaction.Value = id;
				try
				{
				    var documentIdsToTouch = new HashSet<string>();
					foreach (var change in value.Changes)
					{
						var doc = new DocumentInTransactionData
						{
							Metadata = change.Metadata == null ? null : (RavenJObject)change.Metadata.CreateSnapshot(),
							Data = change.Data == null ? null : (RavenJObject)change.Data.CreateSnapshot(),
							Delete = change.Delete,
							Etag = change.Etag,
							LastModified = change.LastModified,
							Key = change.Key
						};

						log.Debug("Prepare of txId {0}: {1} {2}", id, doc.Delete ? "DEL" : "PUT", doc.Key);

						// doc.Etag - represent the _modified_ document etag, and we already
						// checked etags on previous PUT/DELETE, so we don't pass it here
						if (doc.Delete)
						{
							DatabaseDelete(doc.Key, null /* etag might have been changed by a touch */, null);
							documentIdsToTouch.RemoveWhere(x => x.Equals(doc.Key));
						}
						else
						{
							DatabasePut(doc.Key, null /* etag might have been changed by a touch */, doc.Data, doc.Metadata, null);
							documentIdsToTouch.Add(doc.Key);
						}
					}
				    return documentIdsToTouch;
				}
				finally
				{
					currentlyCommittingTransaction.Value = null;
				}
			}
		}

	    public bool RecoverTransaction(string id, IEnumerable<DocumentInTransactionData> changes)
	    {
		    var txInfo = new TransactionInformation
		    {
			    Id = id,
			    Timeout = TimeSpan.FromMinutes(5)
		    };
		    if (changes == null)
		    {
			    log.Warn("Failed to prepare transaction " + id + " because changes were null, maybe this is a partially committed transaction? Transaction will be rolled back");

			    return false;
		    }
		    foreach (var changedDoc in changes)
		    {
			    if (changedDoc == null)
			    {
				    log.Warn("Failed preparing a document change in transaction " + id + " with a null change, maybe this is partiall committed transaction? Transaction will be rolled back");
				    return false;
			    }
			    
				changedDoc.Metadata.EnsureCannotBeChangeAndEnableSnapshotting();
			    changedDoc.Data.EnsureCannotBeChangeAndEnableSnapshotting();

			    //we explicitly pass a null for the etag here, because we might have calls for TouchDocument()
			    //that happened during the transaction, which changed the committed etag. That is fine when we are just running
			    //the transaction, since we can just report the error and abort. But it isn't fine when we recover
			    //var etag = changedDoc.CommittedEtag;
			    Etag etag = null;
			    AddToTransactionState(changedDoc.Key, null, txInfo, etag, changedDoc);
		    }
		    return true;
	    }
	}
}