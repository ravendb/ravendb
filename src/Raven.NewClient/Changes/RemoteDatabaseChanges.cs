using System.Globalization;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Logging;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Json.Linq;
using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.NewClient.Client.Data;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.NewClient.Client.Changes
{
    public class RemoteDatabaseChanges : RemoteChangesClientBase<IDatabaseChanges, DatabaseConnectionState, DocumentConvention>, IDatabaseChanges
    {
        private readonly static ILog Logger = LogManager.GetLogger(typeof(RemoteDatabaseChanges));

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

        protected override void NotifySubscribers(string type, RavenJObject value, List<DatabaseConnectionState> connections)
        {
            switch (type)
            {
                case "DocumentChangeNotification":
                    var documentChangeNotification = value.JsonDeserialization<DocumentChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(documentChangeNotification);
                    }
                    break;

                case "OperationStatusChangeNotification":
                    // using deserializer from Conventions to properly handle $type mapping
                    var operationChangeNotification = Conventions.CreateSerializer().Deserialize<OperationStatusChangeNotification>(new RavenJTokenReader(value));

                    foreach (var counter in connections)
                    {
                        counter.Send(operationChangeNotification);                        
                    }
                    break;

                case "BulkInsertChangeNotification":
                    var bulkInsertChangeNotification = value.JsonDeserialization<BulkInsertChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(bulkInsertChangeNotification);
                    }
                    break;

                case "IndexChangeNotification":
                    var indexChangeNotification = value.JsonDeserialization<IndexChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(indexChangeNotification);
                    }
                    break;
                case "TransformerChangeNotification":
                    var transformerChangeNotification = value.JsonDeserialization<TransformerChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(transformerChangeNotification);
                    }
                    break;
                case "ReplicationConflictNotification":
                    var replicationConflictNotification = value.JsonDeserialization<ReplicationConflictNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(replicationConflictNotification);
                    }

                    if (replicationConflictNotification.ItemType == ReplicationConflictTypes.DocumentReplicationConflict)
                    {
                        tryResolveConflictByUsingRegisteredConflictListenersAsync(replicationConflictNotification.Id,
                            replicationConflictNotification.Etag,
                            replicationConflictNotification.Conflicts, null)
                            .ContinueWith(t =>
                            {
                                t.AssertNotFailed();

                                if (t.Result)
                                {
                                    if (Logger.IsDebugEnabled)
                                        Logger.Debug("Document replication conflict for {0} was resolved by one of the registered conflict listeners",
                                            replicationConflictNotification.Id);
                                }
                            });
                    }

                    break;
                case "DataSubscriptionChangeNotification":
                    var dataSubscriptionChangeNotification = value.JsonDeserialization<DataSubscriptionChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Send(dataSubscriptionChangeNotification);
                    }
                    break;
                default:
                    break;
            }
        }

        public IObservableWithTask<IndexChangeNotification> ForIndex(string indexName)
        {
            var counter = GetOrAddConnectionState("indexes/" + indexName, "watch-index", "unwatch-index", () => watchedIndexes.TryAdd(indexName),
                () => watchedIndexes.TryRemove(indexName), indexName);

            counter.Inc();
            var taskedObservable = new TaskedObservable<IndexChangeNotification, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Name, indexName, StringComparison.OrdinalIgnoreCase));

            counter.OnIndexChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;


            return taskedObservable;
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocument(string docId)
        {
            var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", () => watchedDocs.TryAdd(docId), () => watchedDocs.TryRemove(docId), docId);

            var taskedObservable = new TaskedObservable<DocumentChangeNotification, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Key, docId, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChangeNotification> ForAllDocuments()
        {
            var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", () => watchAllDocs = true, () => watchAllDocs = false, null);

            var taskedObservable = new TaskedObservable<DocumentChangeNotification, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<IndexChangeNotification> ForAllIndexes()
        {
            var counter = GetOrAddConnectionState("all-indexes", "watch-indexes", "unwatch-indexes", () => watchAllIndexes = true, () => watchAllIndexes = false, null);

            var taskedObservable = new TaskedObservable<IndexChangeNotification, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnIndexChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<TransformerChangeNotification> ForAllTransformers()
        {
            var counter = GetOrAddConnectionState("all-transformers", "watch-transformers", "unwatch-transformers", () => watchAllTransformers = true,
                () => watchAllTransformers = false, null);

            var taskedObservable = new TaskedObservable<TransformerChangeNotification, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnTransformerChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix)
        {
            var counter = GetOrAddConnectionState("prefixes/" + docIdPrefix, "watch-prefix", "unwatch-prefix", () => watchedPrefixes.TryAdd(docIdPrefix),
                () => watchedPrefixes.TryRemove(docIdPrefix), docIdPrefix);

            var taskedObservable = new TaskedObservable<DocumentChangeNotification, DatabaseConnectionState>(
                counter,
                notification => notification.Key != null && notification.Key.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection(string collectionName)
        {
            if (collectionName == null) throw new ArgumentNullException("collectionName");

            var counter = GetOrAddConnectionState("collections/" + collectionName, "watch-collection", "unwatch-collection", () => watchedCollections.TryAdd(collectionName),
                () => watchedCollections.TryRemove(collectionName), collectionName);

            var taskedObservable = new TaskedObservable<DocumentChangeNotification, DatabaseConnectionState>(
                counter,
                notification => string.Equals(collectionName, notification.CollectionName, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection<TEntity>()
        {
            var collectionName = Conventions.GetTypeTagName(typeof(TEntity));
            return ForDocumentsInCollection(collectionName);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(string typeName)
        {
            if (typeName == null) throw new ArgumentNullException("typeName");
            var encodedTypeName = Uri.EscapeDataString(typeName);

            var counter = GetOrAddConnectionState("types/" + typeName, "watch-type", "unwatch-type", () => watchedTypes.TryAdd(typeName),
                () => watchedTypes.TryRemove(typeName), encodedTypeName);

            var taskedObservable = new TaskedObservable<DocumentChangeNotification, DatabaseConnectionState>(
                counter,
                notification => string.Equals(typeName, notification.TypeName, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            var typeName = Conventions.FindClrTypeName(type);
            return ForDocumentsOfType(typeName);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType<TEntity>()
        {
            var typeName = Conventions.FindClrTypeName(typeof(TEntity));
            return ForDocumentsOfType(typeName);
        }

        public IObservableWithTask<ReplicationConflictNotification> ForAllReplicationConflicts()
        {
            var counter = GetOrAddConnectionState("all-replication-conflicts", "watch-replication-conflicts", "unwatch-replication-conflicts", () => watchAllIndexes = true, () => watchAllIndexes = false, null);

            var taskedObservable = new TaskedObservable<ReplicationConflictNotification, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnReplicationConflictNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<BulkInsertChangeNotification> ForBulkInsert(Guid? operationId = null)
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

            var taskedObservable = new TaskedObservable<BulkInsertChangeNotification, DatabaseConnectionState>(counter,
                notification => operationId == null || notification.OperationId == operationId);

            counter.OnBulkInsertChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DataSubscriptionChangeNotification> ForAllDataSubscriptions()
        {
            var counter = GetOrAddConnectionState("all-data-subscriptions", "watch-data-subscriptions", "unwatch-data-subscriptions", () => watchAllDataSubscriptions = true, () => watchAllDataSubscriptions = false, null);

            var taskedObservable = new TaskedObservable<DataSubscriptionChangeNotification, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnDataSubscriptionNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<OperationStatusChangeNotification> ForOperationId(long operationId)
        {
            var counter = GetOrAddConnectionState("operations/" + operationId, "watch-operation", "unwatch-operation", 
                () => watchedOperations.TryAdd(operationId), () => watchedOperations.TryRemove(operationId), operationId.ToString(CultureInfo.InvariantCulture));

            var taskedObservable = new TaskedObservable<OperationStatusChangeNotification, DatabaseConnectionState>(
                counter,
                notification => notification.OperationId == operationId);

            counter.OnOperationStatusChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<OperationStatusChangeNotification> ForAllOperations()
        {
            var counter = GetOrAddConnectionState("all-operations", "watch-operations", "unwatch-operations", () => watchAllOperations = true, () => watchAllOperations = false, null);

            var taskedObservable = new TaskedObservable<OperationStatusChangeNotification, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnOperationStatusChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<DataSubscriptionChangeNotification> ForDataSubscription(long subscriptionId)
        {
            var counter = GetOrAddConnectionState("subscriptions/" + subscriptionId, "watch-data-subscription", "unwatch-data-subscription", () => watchedDataSubscriptions.TryAdd(subscriptionId),
                () => watchedDataSubscriptions.TryRemove(subscriptionId), subscriptionId.ToString(CultureInfo.InvariantCulture));

            var taskedObservable = new TaskedObservable<DataSubscriptionChangeNotification, DatabaseConnectionState>(
                counter,
                notification => notification.Id == subscriptionId);

            counter.OnDataSubscriptionNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }
    }
}
