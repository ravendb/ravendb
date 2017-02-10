using System.Globalization;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Logging;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Document;

using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.NewClient.Client.Data;
using Newtonsoft.Json;

namespace Raven.NewClient.Client.Changes
{
    public class RemoteDatabaseChanges //TODO Iftah: RemoteChangesClientBase<IDatabaseChanges, DatabaseConnectionState, DocumentConvention>, IDatabaseChanges
    {
        /*private readonly static ILog Logger = LogManager.GetLogger(typeof(RemoteDatabaseChanges));

        private readonly ConcurrentSet<string> watchedDocs = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedPrefixes = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedTypes = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedCollections = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedIndexes = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedBulkInserts = new ConcurrentSet<string>();
        private readonly ConcurrentSet<long> watchedDataSubscriptions = new ConcurrentSet<long>();
        private readonly ConcurrentSet<long> watchedOperations = new ConcurrentSet<long>();
        private bool watchAllDocs;
        private bool watchAllIndexes;
        private bool watchAllTransformers;
        private bool watchAllDataSubscriptions;
        private bool watchAllOperations;

        private readonly Func<string, long?, string[], OperationMetadata, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync;

        public RemoteDatabaseChanges(string url, string apiKey,
            ICredentials credentials,
            DocumentConvention conventions,
            Action onDispose,
            Func<string, long?, string[], OperationMetadata, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync)
            : base(url, apiKey, credentials, conventions, onDispose)
        {
            this.tryResolveConflictByUsingRegisteredConflictListenersAsync = tryResolveConflictByUsingRegisteredConflictListenersAsync;
        }

        protected override async Task SubscribeOnServer()
        {
            if (watchAllDocs)
                await Send("watch-docs", null).ConfigureAwait(false);

            if (watchAllIndexes)
                await Send("watch-indexes", null).ConfigureAwait(false);

            if (watchAllTransformers)
                await Send("watch-transformers", null).ConfigureAwait(false);

            if (watchAllDataSubscriptions)
                await Send("watch-data-subscriptions", null).ConfigureAwait(false);

            if (watchAllOperations)
                await Send("watch-operations", null).ConfigureAwait(false);

            foreach (var watchedDoc in watchedDocs)
            {
                await Send("watch-doc", watchedDoc).ConfigureAwait(false);
            }

            foreach (var watchedPrefix in watchedPrefixes)
            {
                await Send("watch-prefix", watchedPrefix).ConfigureAwait(false);
            }

            foreach (var watchedCollection in watchedCollections)
            {
                await Send("watch-collection", watchedCollection).ConfigureAwait(false);
            }

            foreach (var watchedType in watchedTypes)
            {
                await Send("watch-type", watchedType).ConfigureAwait(false);
            }

            foreach (var watchedIndex in watchedIndexes)
            {
                await Send("watch-indexes", watchedIndex).ConfigureAwait(false);
            }

            foreach (var watchedBulkInsert in watchedBulkInserts)
            {
                await Send("watch-bulk-operation", watchedBulkInsert).ConfigureAwait(false);
            }

            foreach (var watchedOperation in watchedOperations)
            {
                await Send("watch-operation", watchedOperation.ToString(CultureInfo.InvariantCulture)).ConfigureAwait(false);
            }
        }

        /*protected override void NotifySubscribers(string type, RavenJObject value, List<DatabaseConnectionState> connections)
        {
            switch (type)
            {
                case "DocumentChange":
                    var documentChange = value.JsonDeserialization<DocumentChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(documentChange);
                    }
                    break;

                case "OperationStatusChange":
                    // using deserializer from Conventions to properly handle $type mapping
                    var operationChangeNotification = Conventions.CreateSerializer().Deserialize<OperationStatusChange>(new RavenJTokenReader(value));

                    foreach (var counter in connections)
                    {
                        counter.Send(operationChangeNotification);                        
                    }
                    break;

                case "BulkInsertChange":
                    var bulkInsertChangeNotification = value.JsonDeserialization<BulkInsertChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(bulkInsertChangeNotification);
                    }
                    break;

                case "IndexChange":
                    var indexChange = value.JsonDeserialization<IndexChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(indexChange);
                    }
                    break;
                case "TransformerChange":
                    var transformerChange = value.JsonDeserialization<TransformerChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(transformerChange);
                    }
                    break;
                case "ReplicationConflictChange":
                    var replicationConflictChange = value.JsonDeserialization<ReplicationConflictChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(replicationConflictChange);
                    }

                    if (replicationConflictChange.ItemType == ReplicationConflictTypes.DocumentReplicationConflict)
                    {
                        tryResolveConflictByUsingRegisteredConflictListenersAsync(replicationConflictChange.Id,
                            replicationConflictChange.Etag,
                            replicationConflictChange.Conflicts, null)
                            .ContinueWith(t =>
                            {
                                t.AssertNotFailed();

                                if (t.Result)
                                {
                                    if (Logger.IsDebugEnabled)
                                        Logger.Debug("Document replication conflict for {0} was resolved by one of the registered conflict listeners",
                                            replicationConflictChange.Id);
                                }
                            });
                    }

                    break;
                case "DataSubscriptionChange":
                    var dataSubscriptionChangeNotification = value.JsonDeserialization<DataSubscriptionChange>();
                    foreach (var counter in connections)
                    {
                        counter.Send(dataSubscriptionChangeNotification);
                    }
                    break;
                default:
                    break;
            }
        }#1#

        public IObservableWithTask<IndexChange> ForIndex(string indexName)
        {
            var counter = GetOrAddConnectionState("indexes/" + indexName, "watch-index", "unwatch-index", () => watchedIndexes.TryAdd(indexName),
                () => watchedIndexes.TryRemove(indexName), indexName);

            counter.Inc();
            var taskedObservable = new TaskedObservable<IndexChange, DatabaseConnectionState>(
                counter,
                change => string.Equals(change.Name, indexName, StringComparison.OrdinalIgnoreCase));

            counter.OnIndexChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;


            return taskedObservable;
        }

        public IObservableWithTask<DocumentChange> ForDocument(string docId)
        {
            var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", () => watchedDocs.TryAdd(docId), () => watchedDocs.TryRemove(docId), docId);

            var taskedObservable = new TaskedObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                change => string.Equals(change.Key, docId, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChange> ForAllDocuments()
        {
            var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", () => watchAllDocs = true, () => watchAllDocs = false, null);

            var taskedObservable = new TaskedObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                change => true);

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<IndexChange> ForAllIndexes()
        {
            var counter = GetOrAddConnectionState("all-indexes", "watch-indexes", "unwatch-indexes", () => watchAllIndexes = true, () => watchAllIndexes = false, null);

            var taskedObservable = new TaskedObservable<IndexChange, DatabaseConnectionState>(
                counter,
                change => true);

            counter.OnIndexChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<TransformerChange> ForAllTransformers()
        {
            var counter = GetOrAddConnectionState("all-transformers", "watch-transformers", "unwatch-transformers", () => watchAllTransformers = true,
                () => watchAllTransformers = false, null);

            var taskedObservable = new TaskedObservable<TransformerChange, DatabaseConnectionState>(
                counter,
                change => true);

            counter.OnTransformerChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChange> ForDocumentsStartingWith(string docIdPrefix)
        {
            var counter = GetOrAddConnectionState("prefixes/" + docIdPrefix, "watch-prefix", "unwatch-prefix", () => watchedPrefixes.TryAdd(docIdPrefix),
                () => watchedPrefixes.TryRemove(docIdPrefix), docIdPrefix);

            var taskedObservable = new TaskedObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                change => change.Key != null && change.Key.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChange> ForDocumentsInCollection(string collectionName)
        {
            if (collectionName == null) throw new ArgumentNullException("collectionName");

            var counter = GetOrAddConnectionState("collections/" + collectionName, "watch-collection", "unwatch-collection", () => watchedCollections.TryAdd(collectionName),
                () => watchedCollections.TryRemove(collectionName), collectionName);

            var taskedObservable = new TaskedObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                change => string.Equals(collectionName, change.CollectionName, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChange> ForDocumentsInCollection<TEntity>()
        {
            var collectionName = Conventions.GetTypeTagName(typeof(TEntity));
            return ForDocumentsInCollection(collectionName);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsOfType(string typeName)
        {
            if (typeName == null) throw new ArgumentNullException("typeName");
            var encodedTypeName = Uri.EscapeDataString(typeName);

            var counter = GetOrAddConnectionState("types/" + typeName, "watch-type", "unwatch-type", () => watchedTypes.TryAdd(typeName),
                () => watchedTypes.TryRemove(typeName), encodedTypeName);

            var taskedObservable = new TaskedObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                change => string.Equals(typeName, change.TypeName, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChange> ForDocumentsOfType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            var typeName = Conventions.FindClrTypeName(type);
            return ForDocumentsOfType(typeName);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsOfType<TEntity>()
        {
            var typeName = Conventions.FindClrTypeName(typeof(TEntity));
            return ForDocumentsOfType(typeName);
        }

        public IObservableWithTask<ReplicationConflictChange> ForAllReplicationConflicts()
        {
            var counter = GetOrAddConnectionState("all-replication-conflicts", "watch-replication-conflicts", "unwatch-replication-conflicts", () => watchAllIndexes = true, () => watchAllIndexes = false, null);

            var taskedObservable = new TaskedObservable<ReplicationConflictChange, DatabaseConnectionState>(
                counter,
                change => true);

            counter.OnReplicationConflictNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<BulkInsertChange> ForBulkInsert(Guid? operationId = null)
        {
            var id = operationId != null ? operationId.ToString() : string.Empty;

            var counter = Counters.GetOrAdd("bulk-operations/" + id, s =>
            {
                watchedBulkInserts.TryAdd(id);
                var documentSubscriptionTask = AfterConnection(() =>
                {
                    if (watchedBulkInserts.Contains(id)) // might have been removed in the meantime
                        return Send("watch-bulk-operation", id);
                    return ConnectionTask;
                });

                return new DatabaseConnectionState(
                    () =>
                    {
                        watchedBulkInserts.TryRemove(id);
                        Counters.Remove("bulk-operations/" + operationId);
                        return Send("unwatch-bulk-operation", id);
                    },
                    existingConnectionState =>
                    {
                        DatabaseConnectionState _;
                        if (Counters.TryGetValue("bulk-operations/" + id, out _))
                            return _.Task;

                        Counters.GetOrAdd("bulk-operations/" + id, x => existingConnectionState);

                        return AfterConnection(() =>
                        {
                            if (watchedBulkInserts.Contains(id)) // might have been removed in the meantime
                                return Send("watch-bulk-operation", id);
                            return ConnectionTask;
                        });
                    },
                    documentSubscriptionTask);
            });

            var taskedObservable = new TaskedObservable<BulkInsertChange, DatabaseConnectionState>(counter,
                change => operationId == null || change.OperationId == operationId);

            counter.OnBulkInsertChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DataSubscriptionChange> ForAllDataSubscriptions()
        {
            var counter = GetOrAddConnectionState("all-data-subscriptions", "watch-data-subscriptions", "unwatch-data-subscriptions", () => watchAllDataSubscriptions = true, () => watchAllDataSubscriptions = false, null);

            var taskedObservable = new TaskedObservable<DataSubscriptionChange, DatabaseConnectionState>(
                counter,
                change => true);

            counter.OnDataSubscriptionNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<OperationStatusChange> ForOperationId(long operationId)
        {
            var counter = GetOrAddConnectionState("operations/" + operationId, "watch-operation", "unwatch-operation", 
                () => watchedOperations.TryAdd(operationId), () => watchedOperations.TryRemove(operationId), operationId.ToString(CultureInfo.InvariantCulture));

            var taskedObservable = new TaskedObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                change => change.OperationId == operationId);

            counter.OnOperationStatusChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<OperationStatusChange> ForAllOperations()
        {
            var counter = GetOrAddConnectionState("all-operations", "watch-operations", "unwatch-operations", () => watchAllOperations = true, () => watchAllOperations = false, null);

            var taskedObservable = new TaskedObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                change => true);

            counter.OnOperationStatusChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DataSubscriptionChange> ForDataSubscription(long subscriptionId)
        {
            var counter = GetOrAddConnectionState("subscriptions/" + subscriptionId, "watch-data-subscription", "unwatch-data-subscription", () => watchedDataSubscriptions.TryAdd(subscriptionId),
                () => watchedDataSubscriptions.TryRemove(subscriptionId), subscriptionId.ToString(CultureInfo.InvariantCulture));

            var taskedObservable = new TaskedObservable<DataSubscriptionChange, DatabaseConnectionState>(
                counter,
                change => change.Id == subscriptionId);

            counter.OnDataSubscriptionNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }*/
    }
}
