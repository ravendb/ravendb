using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Iteration;
using Raven.Server.Utils.Enumerators;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseSource : ISmugglerSource
    {
        private readonly DocumentDatabase _database;
        private DocumentsOperationContext _context;
        private TransactionOperationContext _serverContext;

        private readonly long _startDocumentEtag;
        private readonly long _startRaftIndex;
        private readonly Logger _logger;
        private IDisposable _returnContext;
        private IDisposable _returnServerContext;
        private DocumentsTransaction _disposeTransaction;
        private IDisposable _disposeServerTransaction;

        private int _currentTypeIndex;

        private readonly DatabaseItemType[] _types =
        {
            DatabaseItemType.DatabaseRecord,
            DatabaseItemType.Documents,
            DatabaseItemType.RevisionDocuments,
            DatabaseItemType.Tombstones,
            DatabaseItemType.Conflicts,
            DatabaseItemType.Indexes,
            DatabaseItemType.Identities,
            DatabaseItemType.CompareExchange,
            DatabaseItemType.CompareExchangeTombstones,
            DatabaseItemType.CounterGroups,
            DatabaseItemType.Subscriptions,
            DatabaseItemType.TimeSeries,
            DatabaseItemType.ReplicationHubCertificates,
            DatabaseItemType.None
        };

        public long LastEtag { get; private set; }
        public string LastDatabaseChangeVector { get; private set; }
        public long LastRaftIndex { get; private set; }

        private readonly SmugglerSourceType _type;

        public DatabaseSource(DocumentDatabase database, long startDocumentEtag, long startRaftIndex, Logger logger)
        {
            _database = database;
            _startDocumentEtag = startDocumentEtag;
            _startRaftIndex = startRaftIndex;
            _logger = logger;
            _type = _startDocumentEtag == 0 ? SmugglerSourceType.FullExport : SmugglerSourceType.IncrementalExport;
        }

        public IDisposable Initialize(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, out long buildVersion)
        {
            _currentTypeIndex = 0;

            if (options.OperateOnTypes.HasFlag(DatabaseItemType.Documents) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.Conflicts) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.CounterGroups) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.TimeSeries))
            {
                _returnContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
                _disposeTransaction = _context.OpenReadTransaction();
                LastEtag = DocumentsStorage.ReadLastEtag(_disposeTransaction.InnerTransaction);
                LastDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(_disposeTransaction.InnerTransaction);
            }

            if (options.OperateOnTypes.HasFlag(DatabaseItemType.CompareExchange) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.Identities) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.CompareExchangeTombstones) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.Subscriptions) ||
                options.OperateOnTypes.HasFlag(DatabaseItemType.ReplicationHubCertificates))
            {
                _returnServerContext = _database.ServerStore.ContextPool.AllocateOperationContext(out _serverContext);
                _disposeServerTransaction = _serverContext.OpenReadTransaction();

                using (var rawRecord = _database.ServerStore.Cluster.ReadRawDatabaseRecord(_serverContext, _database.Name))
                {
                    LastRaftIndex = rawRecord.EtagForBackup;
                }
            }

            buildVersion = ServerVersion.Build;
            return new DisposableAction(() =>
            {
                _disposeServerTransaction?.Dispose();
                _returnServerContext?.Dispose();

                _disposeTransaction?.Dispose();
                _returnContext?.Dispose();
            });
        }

        public DatabaseItemType GetNextType()
        {
            return _types[_currentTypeIndex++];
        }

        public DatabaseRecord GetDatabaseRecord()
        {
            var databaseRecord = _database.ReadDatabaseRecord();

            // filter server-wide backup tasks
            for (var i = databaseRecord.PeriodicBackups.Count - 1; i >= 0; i--)
            {
                var periodicBackup = databaseRecord.PeriodicBackups[i];
                if (periodicBackup.Name == null)
                    continue;

                if (periodicBackup.Name.StartsWith(ServerWideBackupConfiguration.NamePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    databaseRecord.PeriodicBackups.RemoveAt(i);
                }
            }

            // filter server-wide external replication tasks
            for (var i = databaseRecord.ExternalReplications.Count - 1; i >= 0; i--)
            {
                var periodicBackup = databaseRecord.ExternalReplications[i];
                if (periodicBackup.Name == null)
                    continue;

                if (periodicBackup.Name.StartsWith(ServerWideExternalReplication.NamePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    databaseRecord.ExternalReplications.RemoveAt(i);
                }
            }

            return databaseRecord;
        }

        public IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            Debug.Assert(_context != null);

            var enumerator = new PulsedTransactionEnumerator<Document, DocumentsIterationState>(_context,
                state =>
                {
                    if (state.StartEtagByCollection.Count != 0)
                        return GetDocumentsFromCollections(_context, state);

                    return _database.DocumentsStorage.GetDocumentsFrom(_context, state.StartEtag, 0, long.MaxValue);
                },
                new DocumentsIterationState(_context, _database.Configuration.Databases.PulseReadTransactionLimit) // initial state
                {
                    StartEtag = _startDocumentEtag,
                    StartEtagByCollection = collectionsToExport.ToDictionary(x => x, x => _startDocumentEtag)
                });

            while (enumerator.MoveNext())
            {
                yield return new DocumentItem
                {
                    Document = enumerator.Current
                };
            }
        }

        private IEnumerable<Document> GetDocumentsFromCollections(DocumentsOperationContext context, DocumentsIterationState state)
        {
            var collections = state.StartEtagByCollection.Keys.ToList();

            foreach (var collection in collections)
            {
                var etag = state.StartEtagByCollection[collection];

                state.CurrentCollection = collection;

                foreach (var document in _database.DocumentsStorage.GetDocumentsFrom(context, collection, etag, 0, long.MaxValue))
                {
                    yield return document;
                }
            }
        }

        public IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            Debug.Assert(_context != null);

            var revisionsStorage = _database.DocumentsStorage.RevisionsStorage;
            if (revisionsStorage.Configuration == null)
                yield break;

            var enumerator = new PulsedTransactionEnumerator<Document, DocumentsIterationState>(_context,
                state =>
                {
                    if (state.StartEtagByCollection.Count != 0)
                        return GetRevisionsFromCollections(_context, state);

                    return revisionsStorage.GetRevisionsFrom(_context, state.StartEtag, long.MaxValue);
                },
                new DocumentsIterationState(_context, _database.Configuration.Databases.PulseReadTransactionLimit) // initial state
                {
                    StartEtag = _startDocumentEtag,
                    StartEtagByCollection = collectionsToExport.ToDictionary(x => x, x => _startDocumentEtag)
                });

            while (enumerator.MoveNext())
            {
                yield return new DocumentItem
                {
                    Document = enumerator.Current
                };
            }
        }

        private IEnumerable<Document> GetRevisionsFromCollections(DocumentsOperationContext context, DocumentsIterationState state)
        {
            var collections = state.StartEtagByCollection.Keys.ToList();

            foreach (var collection in collections)
            {
                var etag = state.StartEtagByCollection[collection];

                var collectionName = _database.DocumentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
                if (collectionName == null)
                    continue;

                state.CurrentCollection = collection;

                foreach (var document in _database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, collectionName, etag, long.MaxValue))
                {
                    yield return document.current;
                }
            }
        }

        public IEnumerable<DocumentItem> GetLegacyAttachments(INewDocumentActions actions)
        {
            return Enumerable.Empty<DocumentItem>();
        }

        public IEnumerable<string> GetLegacyAttachmentDeletions()
        {
            return Enumerable.Empty<string>();
        }

        public IEnumerable<string> GetLegacyDocumentDeletions()
        {
            return Enumerable.Empty<string>();
        }

        public Stream GetAttachmentStream(LazyStringValue hash, out string tag)
        {
            Debug.Assert(_context != null);

            using (Slice.External(_context.Allocator, hash, out Slice hashSlice))
            {
                return _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(_context, hashSlice, out tag);
            }
        }

        public IEnumerable<Tombstone> GetTombstones(List<string> collectionsToExport, INewDocumentActions actions)
        {
            Debug.Assert(_context != null);

            var enumerator = new PulsedTransactionEnumerator<Tombstone, TombstonesIterationState>(_context,
                state =>
                {
                    if (state.StartEtagByCollection.Count != 0)
                        return GetTombstonesFromCollections(_context, state);

                    return _database.DocumentsStorage.GetTombstonesFrom(_context, state.StartEtag, 0, long.MaxValue);
                },
                new TombstonesIterationState(_context, _database.Configuration.Databases.PulseReadTransactionLimit)
                {
                    StartEtag = _startDocumentEtag,
                    StartEtagByCollection = collectionsToExport.ToDictionary(x => x, x => _startDocumentEtag)
                });

            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }

        private IEnumerable<Tombstone> GetTombstonesFromCollections(DocumentsOperationContext context, TombstonesIterationState state)
        {
            var collections = state.StartEtagByCollection.Keys.ToList();

            foreach (var collection in collections)
            {
                var etag = state.StartEtagByCollection[collection];

                state.CurrentCollection = collection;

                foreach (var counter in _database.DocumentsStorage.GetTombstonesFrom(context, collection, etag, 0, long.MaxValue))
                {
                    yield return counter;
                }
            }
        }

        public IEnumerable<DocumentConflict> GetConflicts(List<string> collectionsToExport, INewDocumentActions actions)
        {
            Debug.Assert(_context != null);

            var enumerator = new PulsedTransactionEnumerator<DocumentConflict, DocumentConflictsIterationState>(_context,
                state =>
                {
                    if (collectionsToExport.Count != 0)
                        return GetConflictsFromCollections(_context, collectionsToExport.ToHashSet(), state);

                    return _database.DocumentsStorage.ConflictsStorage.GetConflictsFrom(_context, state.StartEtag);
                },
                new DocumentConflictsIterationState(_context, _database.Configuration.Databases.PulseReadTransactionLimit)
                {
                    StartEtag = _startDocumentEtag,
                });

            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }

        private IEnumerable<DocumentConflict> GetConflictsFromCollections(DocumentsOperationContext context, HashSet<string> collections, DocumentConflictsIterationState state)
        {
            foreach (var conflict in _database.DocumentsStorage.ConflictsStorage.GetConflictsFrom(context, state.StartEtag))
            {
                if (collections.Contains(conflict.Collection) == false)
                {
                    state.StartEtag = conflict.Etag + 1; // when skipping an item we need to increment StartEtag
                    continue;
                }

                yield return conflict;
            }
        }

        public IEnumerable<IndexDefinitionAndType> GetIndexes()
        {
            var allIndexes = _database.IndexStore.GetIndexes().ToList();
            var sideBySideIndexes = allIndexes.Where(x => x.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix)).ToList();

            var originalSideBySideIndexNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var index in sideBySideIndexes)
            {
                allIndexes.Remove(index);

                if (index.Type == IndexType.Faulty)
                    continue;

                var indexName = index.Name.Remove(0, Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length);
                originalSideBySideIndexNames.Add(indexName);
                var indexDefinition = index.GetIndexDefinition();
                indexDefinition.Name = indexName;

                yield return new IndexDefinitionAndType
                {
                    IndexDefinition = indexDefinition,
                    Type = index.Type
                };
            }

            foreach (var index in allIndexes)
            {
                if (originalSideBySideIndexNames.Contains(index.Name))
                    continue;

                if (index.Type == IndexType.Faulty)
                    continue;

                if (index.Type.IsStatic())
                {
                    yield return new IndexDefinitionAndType
                    {
                        IndexDefinition = index.GetIndexDefinition(),
                        Type = index.Type
                    };

                    continue;
                }

                yield return new IndexDefinitionAndType
                {
                    IndexDefinition = index.Definition,
                    Type = index.Type
                };
            }
        }

        public IEnumerable<(string Prefix, long Value, long Index)> GetIdentities()
        {
            Debug.Assert(_serverContext != null);

            return _database.ServerStore.Cluster.GetIdentitiesFromPrefix(_serverContext, _database.Name, _startRaftIndex, long.MaxValue);
        }

        public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValues()
        {
            Debug.Assert(_serverContext != null);

            return _database.ServerStore.Cluster.GetCompareExchangeFromPrefix(_serverContext, _database.Name, _startRaftIndex, long.MaxValue);
        }

        public IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstones()
        {
            Debug.Assert(_serverContext != null);

            return _database.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(_serverContext, _database.Name);
        }

        public IEnumerable<CounterGroupDetail> GetCounterValues(List<string> collectionsToExport, ICounterActions actions)
        {
            Debug.Assert(_context != null);

            var enumerator = new PulsedTransactionEnumerator<CounterGroupDetail, CountersIterationState>(_context,
                state =>
                {
                    if (state.StartEtagByCollection.Count != 0)
                        return GetCounterValuesFromCollections(_context, state);

                    return _database.DocumentsStorage.CountersStorage.GetCountersFrom(_context, state.StartEtag, 0, long.MaxValue);
                },
                new CountersIterationState(_context, _database.Configuration.Databases.PulseReadTransactionLimit) // initial state
                {
                    StartEtag = _startDocumentEtag,
                    StartEtagByCollection = collectionsToExport.ToDictionary(x => x, x => _startDocumentEtag)
                });

            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }

        private IEnumerable<CounterGroupDetail> GetCounterValuesFromCollections(DocumentsOperationContext context, CountersIterationState state)
        {
            var collections = state.StartEtagByCollection.Keys.ToList();

            foreach (var collection in collections)
            {
                var etag = state.StartEtagByCollection[collection];

                state.CurrentCollection = collection;

                foreach (var counter in _database.DocumentsStorage.CountersStorage.GetCountersFrom(context, collection, etag, 0, long.MaxValue))
                {
                    yield return counter;
                }
            }
        }

        public IEnumerable<CounterDetail> GetLegacyCounterValues()
        {
            // used only in StreamSource
            return Enumerable.Empty<CounterDetail>();
        }

        public IEnumerable<(string Hub, ReplicationHubAccess Access)> GetReplicationHubCertificates()
        {
            return _database.ServerStore.Cluster.GetReplicationHubCertificateForDatabase(_serverContext, _database.Name);
        }

        public IEnumerable<SubscriptionState> GetSubscriptions()
        {
            Debug.Assert(_serverContext != null);

            return _database.SubscriptionStorage.GetAllSubscriptions(_serverContext, false, 0, int.MaxValue);
        }

        public IEnumerable<TimeSeriesItem> GetTimeSeries(List<string> collectionsToExport)
        {
            Debug.Assert(_context != null);

            var initialState = new TimeSeriesIterationState(_context, _database.Configuration.Databases.PulseReadTransactionLimit)
            {
                StartEtag = _startDocumentEtag,
                StartEtagByCollection = collectionsToExport.ToDictionary(x => x, x => _startDocumentEtag)
            };

            var enumerator = new PulsedTransactionEnumerator<TimeSeriesItem, TimeSeriesIterationState>(_context,
                state =>
                {
                    if (state.StartEtagByCollection.Count != 0)
                        return GetTimeSeriesFromCollections(_context, state);

                    return GetAllTimeSeriesItems(_context, state.StartEtag);
                }, initialState);

            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }

        private static IEnumerable<TimeSeriesItem> GetAllTimeSeriesItems(DocumentsOperationContext context, long startEtag)
        {
            var database = context.DocumentDatabase;
            foreach (var ts in database.DocumentsStorage.TimeSeriesStorage.GetTimeSeriesFrom(context, startEtag, long.MaxValue))
            {
                yield return new TimeSeriesItem
                {
                    Name = database.DocumentsStorage.TimeSeriesStorage.GetOriginalName(context, ts.DocId, ts.Name),
                    DocId = ts.DocId,
                    Baseline = ts.Start,
                    ChangeVector = ts.ChangeVector,
                    Collection = ts.Collection,
                    SegmentSize = ts.SegmentSize,
                    Segment = ts.Segment,
                    Etag = ts.Etag
                };
            }
        }

        private static IEnumerable<TimeSeriesItem> GetTimeSeriesFromCollections(DocumentsOperationContext context, TimeSeriesIterationState state)
        {
            var database = context.DocumentDatabase;
            var collections = state.StartEtagByCollection.Keys.ToList();

            foreach (var collection in collections)
            {
                var etag = state.StartEtagByCollection[collection];

                state.CurrentCollection = collection;

                foreach (var ts in database.DocumentsStorage.TimeSeriesStorage.GetTimeSeriesFrom(context, collection, etag, long.MaxValue))
                {
                    yield return new TimeSeriesItem
                    {
                        Name = database.DocumentsStorage.TimeSeriesStorage.GetOriginalName(context, ts.DocId, ts.Name),
                        DocId = ts.DocId,
                        Baseline = ts.Start,
                        ChangeVector = ts.ChangeVector,
                        Collection = ts.Collection,
                        SegmentSize = ts.SegmentSize,
                        Segment = ts.Segment,
                        Etag = ts.Etag,
                    };
                }
            }
        }

        public long SkipType(DatabaseItemType type, Action<long> onSkipped, CancellationToken token)
        {
            return 0; // no-op
        }

        public SmugglerSourceType GetSourceType()
        {
            return _type;
        }
    }
}
