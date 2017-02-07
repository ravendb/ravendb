// -----------------------------------------------------------------------
//  <copyright file="InFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
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

namespace Raven.Database.Impl.DTC
{
    public abstract class InFlightTransactionalState:IDisposable
    {
        protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        protected readonly Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> DatabasePut;
        protected readonly Func<string, Etag, TransactionInformation, bool> DatabaseDelete;
        private readonly bool replicationEnabled;
        
        private readonly object modifyChangedInTransaction = new object();

        protected ImmutableDictionary<string, ChangedDoc> changedInTransaction = ImmutableDictionary<string, ChangedDoc>.Empty;

        private readonly object modifyTransactionStates = new object();

        protected ImmutableDictionary<string, TransactionState> transactionStates = ImmutableDictionary<string, TransactionState>.Empty;

        public object GetInFlightTransactionsInternalStateForDebugOnly()
        {
            return new { changedInTransaction, transactionStates };
        }

        protected InFlightTransactionalState(Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, Func<string, Etag, TransactionInformation, bool> databaseDelete, bool replicationEnabled)
        {
            this.DatabasePut = databasePut;
            this.DatabaseDelete = databaseDelete;
            this.replicationEnabled = replicationEnabled;
        }

        public virtual Etag AddDocumentInTransaction(
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

        public virtual void DeleteDocumentInTransaction(
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

        public virtual bool IsModified(string key)
        {
            var value = currentlyCommittingTransaction.Value;
            if (string.IsNullOrEmpty(value))
                return changedInTransaction.ContainsKey(key);
            ChangedDoc doc;
            if (changedInTransaction.TryGetValue(key, out doc) == false)
                return false;
            return doc.transactionId != value;
        }

        public abstract IInFlightStateSnapshot GetSnapshot();

        public virtual void Rollback(string id)
        {
            TransactionState value;
            if (transactionStates.TryGetValue(id, out value) == false)
                return;

            lock (modifyTransactionStates)
            {
                transactionStates = transactionStates.Remove(id);
            }

            lock (value)
            {
                lock (modifyChangedInTransaction)
                {
                    foreach (var change in value.Changes)
                    {
                        changedInTransaction = changedInTransaction.Remove(change.Key);
                    }
                }
                
                value.Changes.Clear();
            }
        }

        protected readonly ThreadLocal<string> currentlyCommittingTransaction = new ThreadLocal<string>();

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
                TransactionState state;

                if (transactionStates.TryGetValue(transactionInformation.Id, out state) == false)
                {
                    lock (modifyTransactionStates)
                    {
                        if (transactionStates.TryGetValue(transactionInformation.Id, out state) == false) // check it once again, after we retrieved the lock - could be added while we waited for the lock
                        {
                            state = new TransactionState();
                            transactionStates = transactionStates.Add(transactionInformation.Id, state);
                        }
                    }
                }
                
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

                    ChangedDoc result;

                    lock (modifyChangedInTransaction)
                    {
                        if (changedInTransaction.TryGetValue(key, out result))
                        {
                            if (result.transactionId != transactionInformation.Id)
                                throw new ConcurrencyException("Document " + key + " is being modified by another transaction: " + result);

                            EnsureValidEtag(key, etag, committedEtag, currentTxVal);
                            result.currentEtag = item.Etag;
                        }
                        else
                        {
                            EnsureValidEtag(key, etag, committedEtag, currentTxVal);

                            result = new ChangedDoc
                            {
                                transactionId = transactionInformation.Id,
                                committedEtag = committedEtag,
                                currentEtag = item.Etag
                            };

                            changedInTransaction = changedInTransaction.Add(key, result);
                        }
                    }

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

        public virtual bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocument document)
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

        public virtual bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocumentMetadata document)
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

        public virtual bool HasTransaction(string txId)
        {
            return transactionStates.ContainsKey(txId);
        }

        protected ItemsToTouch RunOperationsInTransaction(string id, out List<DocumentInTransactionData> changes)
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
                    var itemsToTouch = new ItemsToTouch();

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

                        if (log.IsDebugEnabled)
                            log.Debug("Prepare of txId {0}: {1} {2}", id, doc.Delete ? "DEL" : "PUT", doc.Key);

                        // doc.Etag - represent the _modified_ document etag, and we already
                        // checked etags on previous PUT/DELETE, so we don't pass it here
                        if (doc.Delete)
                        {
                            DatabaseDelete(doc.Key, null /* etag might have been changed by a touch */, null);
                            itemsToTouch.Documents.RemoveWhere(x => x.Equals(doc.Key));

                            if (replicationEnabled)
                            {
                                itemsToTouch.DocumentTombstones.Add(doc.Key);
                            }
                        }
                        else
                        {
                            DatabasePut(doc.Key, null /* etag might have been changed by a touch */, doc.Data, doc.Metadata, null);
                            itemsToTouch.Documents.Add(doc.Key);
                        }
                    }
                    return itemsToTouch;
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

        public void Dispose()
        {
            this.currentlyCommittingTransaction.Dispose();
        }
    }
}
