using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    public class RemoteDatabaseChanges : RemoteChangesClientBase<IDatabaseChanges, DatabaseConnectionState>, IDatabaseChanges
    {
        private readonly static ILog log = LogManager.GetCurrentClassLogger();

        private readonly ConcurrentSet<string> watchedDocs = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedPrefixes = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedTypes = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedCollections = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedIndexes = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedBulkInserts = new ConcurrentSet<string>();
        private readonly ConcurrentSet<long> watchedDataSubscriptions = new ConcurrentSet<long>();
        private bool watchAllDocs;
        private bool watchAllIndexes;
        private bool watchAllTransformers;
        private bool watchAllDataSubscriptions;

        private readonly Func<string, Etag, string[], OperationMetadata, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync;

        protected readonly DocumentConvention Conventions;

        public RemoteDatabaseChanges(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, DocumentConvention conventions,
                                       IReplicationInformerBase replicationInformer,
                                       Action onDispose,
                                       Func<string, Etag, string[], OperationMetadata, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync)
            : base(url, apiKey, credentials, jsonRequestFactory, conventions, replicationInformer, onDispose)
        {
            this.Conventions = conventions;
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
                                    log.Debug("Document replication conflict for {0} was resolved by one of the registered conflict listeners",
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
                notification => string.Equals(notification.Id, docId, StringComparison.OrdinalIgnoreCase));

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
                notification => notification.Id != null && notification.Id.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

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
                    return Task;
                });

                return new DatabaseConnectionState(
                    () =>
                    {
                        watchedBulkInserts.TryRemove(id);
                        Send("unwatch-bulk-operation", id);
                        Counters.Remove("bulk-operations/" + operationId);
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
                            return Task;
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

        private DatabaseConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, Action afterConnection, Action beforeDisconnect, string value)
        {
            var counter = Counters.GetOrAdd(name, s =>
            {
                var documentSubscriptionTask = AfterConnection(() =>
                {
                    afterConnection();
                    return Send(watchCommand, value);
                });

                return new DatabaseConnectionState(
                    () =>
                    {
                        beforeDisconnect();
                        Send(unwatchCommand, value);
                        Counters.Remove(name);
                    },
                    existingConnectionState =>
                    {
                        DatabaseConnectionState _;
                        if (Counters.TryGetValue(name, out _))
                            return _.Task;

                        Counters.GetOrAdd(name, x => existingConnectionState);

                        return AfterConnection(() =>
                        {
                            afterConnection();
                            return Send(watchCommand, value);
                        });
                    },
                    documentSubscriptionTask);
            });

            return counter;
        }

        private Task AfterConnection(Func<Task> action)
        {
            return Task.ContinueWith(task =>
            {
                task.AssertNotFailed();
                return action();
            })
            .Unwrap();
        }

    }
}
