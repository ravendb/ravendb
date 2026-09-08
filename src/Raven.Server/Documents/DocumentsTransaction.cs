using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Changes;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public sealed class DocumentsTransaction : RavenTransaction
    {
        private readonly DocumentsOperationContext _context;

        private readonly DocumentsChanges _changes;

        private List<DocumentChange> _documentNotifications;

        private List<CounterChange> _counterNotifications;

        private List<TimeSeriesChange> _timeSeriesNotifications;

        private List<Slice> _attachmentHashesToMaybeDelete;

        private bool _executeDocumentsMigrationAfterCommit;

        private bool _replaced;

        private Dictionary<string, CollectionName> _collectionCache;

        private long _putsCount; 
        private long _putsBytes;

        // at scale, calling Mark() is expensive (vdso clock read), instead of doing per doc, we'll aggregate and call once
        public void AccumulatePutMetrics(long documentSize)
        {
            _putsCount++;
            _putsBytes += documentSize;
        }

        private long _cachedLastModifiedTicks;

        // All documents written by this transaction share one LastModified reading, saves a vdso clock read per doc
        public long GetOrCreateLastModifiedTicks()
        {
            if (_cachedLastModifiedTicks == 0)
                _cachedLastModifiedTicks = _context.DocumentDatabase.Time.GetUtcNow().Ticks;
            return _cachedLastModifiedTicks;
        }

        private CollectionTables[] _collectionTables = Array.Empty<CollectionTables>();

        private struct CollectionTables
        {
            public Voron.Data.Tables.Table Documents;
            public Voron.Data.Tables.Table CompressedDocuments;
            public Voron.Data.Tables.Table Tombstones;
        }

        private CollectionTables[] GrowCollectionTables(int index)
        {
            InnerTransaction.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache);
            int size = Math.Max(cache?.Collections?.Count ?? 0, index + 1);
            Array.Resize(ref _collectionTables, size);
            return _collectionTables;
        }
        
        public Voron.Data.Tables.Table GetOrOpenDocumentsTable(CollectionName collection, Voron.Data.Tables.TableSchema schema)
        {
            ref CollectionTables entry = ref GetTableEntry(collection);
            ref var slot = ref (schema.Compressed ? ref entry.CompressedDocuments : ref entry.Documents);
            return slot ??= InnerTransaction.OpenTable(schema, collection.GetTableName(CollectionTableType.Documents));
        }

        public Voron.Data.Tables.Table GetOrOpenTombstonesTable(CollectionName collection, Voron.Data.Tables.TableSchema tombstonesSchema)
        {
            ref CollectionTables entry = ref GetTableEntry(collection);
            return entry.Tombstones ??= InnerTransaction.OpenTable(tombstonesSchema, collection.GetTableName(CollectionTableType.Tombstones));
        }

        private ref CollectionTables GetTableEntry(CollectionName collection)
        {
            int index = collection.Index;
            if (index < 0)
            {
                var tmp = new CollectionTables[1]; // should be rare
                return ref tmp[0];
            }

            var tables = _collectionTables;
            if (tables == null || (uint)index >= (uint)tables.Length)
                tables = GrowCollectionTables(index);

            return ref tables[index];
        }

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, DocumentsChanges changes)
            : base(transaction)
        {
            _context = context;
            _changes = changes;

            // ComputeTransactionCache runs at the Voron layer and reaches back to this documents transaction
            // (for the collections created in it) through Transaction.Owner, so it must be set on every tx.
            transaction.Owner = _context;

            if (context.DocumentDatabase is ShardedDocumentDatabase sharded)
            {
                transaction.OnBeforeCommit += sharded.ShardedDocumentsStorage.OnBeforeCommit;
                transaction.LowLevelTransaction.OnRollBack += sharded.ShardedDocumentsStorage.OnFailure;
            }
        }

        public override void BeforeCommit()
        {
            if (_putsCount != 0)
            {
                var docsMetrics = _context.DocumentDatabase.Metrics.Docs;
                docsMetrics.PutsPerSec.MarkSingleThreaded(_putsCount);
                docsMetrics.BytesPutsPerSec.MarkSingleThreaded(_putsBytes);
                _putsCount = 0;
                _putsBytes = 0;
            }

            if (_attachmentHashesToMaybeDelete == null)
                return;

            _context.DocumentDatabase.DocumentsStorage.AttachmentsStorage.RemoveAttachmentStreamsWithoutReferences(_context, _attachmentHashesToMaybeDelete);
        }

        protected override void AfterCommit()
        {
            if (_executeDocumentsMigrationAfterCommit)
            {
                var shardedDatabase = ShardedDocumentDatabase.CastToShardedDocumentDatabase(_context.DocumentDatabase);
                shardedDatabase.DocumentsMigrator.ExecuteMoveDocumentsAsync().IgnoreUnobservedExceptions();
            }

            base.AfterCommit();
        }

        public DocumentsTransaction BeginAsyncCommitAndStartNewTransaction(DocumentsOperationContext context)
        {
            BeforeCommit();
            _replaced = true;
            _context.ResetTablesCache();
            var tx = InnerTransaction.BeginAsyncCommitAndStartNewTransaction(context.PersistentContext);
            return new DocumentsTransaction(context, tx, _changes);
        }

        // Internal listeners (indexing, ETL, replication, subscriptions) only need to know which
        // *collections* changed, what changed in each (documents / counters / time series), and
        // whether all of it came from incoming replication
        private Dictionary<string, ChangedCollection> _changedCollections;

        private struct ChangedCollection
        {
            public DocumentChangeTypes DocumentTypes; 
            public bool HasCounters;
            public bool HasTimeSeries;
            public bool DocumentsReplicationOnly;
            public bool CountersReplicationOnly;
            public bool TimeSeriesReplicationOnly;
        }

        private ref ChangedCollection GetChangedCollection(string collectionName)
        {
            var changedCollections = _changedCollections ??= new Dictionary<string, ChangedCollection>(StringComparer.Ordinal);
            return ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(changedCollections, collectionName, out _);
        }

        public void AddAfterCommitNotification(string collectionName, string id, string changeVector, DocumentChangeTypes type)
        {
            var triggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            ref ChangedCollection changed = ref GetChangedCollection(collectionName);
            changed.DocumentsReplicationOnly = triggeredByReplicationThread && (changed.DocumentTypes == DocumentChangeTypes.None || changed.DocumentsReplicationOnly);
            changed.DocumentTypes |= type;

            if (_changes.HasConnections == false)
                return;

            (_documentNotifications ??= []).Add(new DocumentChange
            {
                ChangeVector = changeVector,
                CollectionName = collectionName,
                Id = id,
                Type = type,
                TriggeredByReplicationThread = triggeredByReplicationThread
            });
        }

        public void AddAfterCommitNotification(string collectionName, string documentId, string counterName, string changeVector, CounterChangeTypes type, long value = 0)
        {
            var triggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            ref ChangedCollection changed = ref GetChangedCollection(collectionName);
            changed.CountersReplicationOnly = triggeredByReplicationThread && (changed.HasCounters == false || changed.CountersReplicationOnly);
            changed.HasCounters = true;

            if (_changes.HasConnections == false)
                return; // the per-item change object is only needed for connected /changes clients

            (_counterNotifications ??= []).Add(new CounterChange
            {
                CollectionName = collectionName,
                DocumentId = documentId,
                Name = counterName,
                ChangeVector = changeVector,
                Type = type,
                Value = value,
                TriggeredByReplicationThread = triggeredByReplicationThread
            });
        }

        public void AddAfterCommitNotification(string collectionName, string documentId, string timeSeriesName, string changeVector, TimeSeriesChangeTypes type, DateTime from, DateTime to)
        {
            var triggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            ref ChangedCollection changed = ref GetChangedCollection(collectionName);
            changed.TimeSeriesReplicationOnly = triggeredByReplicationThread && (changed.HasTimeSeries == false || changed.TimeSeriesReplicationOnly);
            changed.HasTimeSeries = true;

            if (_changes.HasConnections == false)
                return; // the per-item change object is only needed for connected /changes clients

            (_timeSeriesNotifications ??= []).Add(new TimeSeriesChange
            {
                CollectionName = collectionName,
                DocumentId = documentId,
                Name = timeSeriesName,
                ChangeVector = changeVector,
                Type = type,
                From = from,
                To = to,
                TriggeredByReplicationThread = triggeredByReplicationThread
            });
        }

        private bool _isDisposed;

        public override void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            if (_replaced == false)
            {
                if (_context.Transaction != null && _context.Transaction != this)
                    ThrowInvalidTransactionUsage();

                _context.ResetTablesCache();
                _context.Transaction = null;
            }

            base.Dispose();
        }

        protected override void RaiseNotifications()
        {
            base.RaiseNotifications();

            if (_changedCollections != null)
            {
                // one wakeup per changed collection per domain for internal listeners
                foreach (var (collection, changed) in _changedCollections)
                {
                    if (changed.DocumentTypes != DocumentChangeTypes.None)
                    {
                        _changes.RaiseInternalDocumentChangeNotification(new DocumentChange
                        {
                            CollectionName = collection,
                            Type = changed.DocumentTypes,
                            TriggeredByReplicationThread = changed.DocumentsReplicationOnly
                        });
                    }

                    if (changed.HasCounters)
                    {
                        _changes.RaiseInternalCounterChangeNotification(new CounterChange
                        {
                            CollectionName = collection,
                            TriggeredByReplicationThread = changed.CountersReplicationOnly
                        });
                    }

                    if (changed.HasTimeSeries)
                    {
                        _changes.RaiseInternalTimeSeriesChangeNotification(new TimeSeriesChange
                        {
                            CollectionName = collection,
                            TriggeredByReplicationThread = changed.TimeSeriesReplicationOnly
                        });
                    }
                }
            }

            if (_documentNotifications?.Count > 0)
            {
                // per-document detail for connected /changes clients
                foreach (var notification in _documentNotifications)
                {
                    _changes.SendDocumentChangeToConnections(notification);
                }
            }

            if (_counterNotifications?.Count > 0)
            {
                foreach (var notification in _counterNotifications)
                {
                    _changes.SendCounterChangeToConnections(notification);
                }
            }

            if (_timeSeriesNotifications?.Count > 0)
            {
                foreach (var notification in _timeSeriesNotifications)
                {
                    _changes.SendTimeSeriesChangeToConnections(notification);
                }
            }
        }

        protected override bool ShouldRaiseNotifications()
        {
            return base.ShouldRaiseNotifications()
                || _changedCollections != null;
        }

        // the collections created (first seen) in this transaction, with canonical CollectionName instances.
        // populated synchronously by ExtractCollectionName, so it is complete while the tx is committing.
        public IEnumerable<CollectionName> CollectionsCreatedInTransaction => _collectionCache?.Values;

        public bool TryGetFromCache(string collectionName, out CollectionName name)
        {
            if (_collectionCache != null)
                return _collectionCache.TryGetValue(collectionName, out name);

            name = null;
            return false;
        }

        public void AddToCache(string collectionName, CollectionName name)
        {
            if (_collectionCache == null)
                _collectionCache = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

            _collectionCache.Add(collectionName, name);
        }

        internal void CheckIfShouldDeleteAttachmentStream(Slice hash)
        {
            var clone = hash.Clone(InnerTransaction.Allocator);
            _attachmentHashesToMaybeDelete ??= new();
            _attachmentHashesToMaybeDelete.Add(clone);
        }

        internal void ExecuteDocumentsMigrationAfterCommit()
        {
            _executeDocumentsMigrationAfterCommit = true;
        }
    }
}
