using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Actions;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Utils;
using Voron;
using Voron.Global;
using Size = Sparrow.Size;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseDestination : ISmugglerDestination
    {
        protected readonly DocumentDatabase _database;
        protected readonly CancellationToken _token;
        internal DuplicateDocsHandler _duplicateDocsHandler;

        private readonly Logger _log;
        private BuildVersionType _buildType;
        private DatabaseSmugglerOptionsServerSide _options;
        protected SmugglerResult _result;
        protected Action<IOperationProgress> _onProgress;

        public DatabaseDestination(DocumentDatabase database, CancellationToken token = default)
        {
            _database = database;
            _token = token;
            _log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
            _duplicateDocsHandler = new DuplicateDocsHandler(_database);
        }

        public ValueTask<IAsyncDisposable> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, Action<IOperationProgress> onProgress, long buildVersion)
        {
            _buildType = BuildVersion.Type(buildVersion);
            _options = options;
            _result = result;
            _onProgress = onProgress;

            var d = new AsyncDisposableAction(() =>
            {
                _duplicateDocsHandler.Dispose();
                return Task.CompletedTask;
            });

            return ValueTask.FromResult<IAsyncDisposable>(d);
        }

        public IDatabaseRecordActions DatabaseRecord()
        {
            return new DatabaseRecordActions(_database, log: _log);
        }

        public IDocumentActions Documents(bool throwOnCollectionMismatchError = true)
        {
            return new DatabaseDocumentActions(_database, _buildType, _options, isRevision: false, _log, _duplicateDocsHandler, throwOnCollectionMismatchError);
        }

        public IDocumentActions RevisionDocuments()
        {
            return new DatabaseDocumentActions(_database, _buildType, _options, isRevision: true, _log, _duplicateDocsHandler, throwOnCollectionMismatchError: true);
        }

        public IDocumentActions Tombstones()
        {
            return new DatabaseDocumentActions(_database, _buildType, _options, isRevision: false, _log, _duplicateDocsHandler, throwOnCollectionMismatchError: true);
        }

        public IDocumentActions Conflicts()
        {
            return new DatabaseDocumentActions(_database, _buildType, _options, isRevision: false, _log, _duplicateDocsHandler, throwOnCollectionMismatchError: true);
        }

        public IKeyValueActions<long> Identities()
        {
            return new DatabaseKeyValueActions(_database);
        }

        public ICompareExchangeActions CompareExchange(string databaseName, JsonOperationContext context, BackupKind? backupKind, bool withDocuments)
        {
            if (withDocuments == false)
                return CreateCompareExchangeActions(databaseName, context, backupKind);

            switch (backupKind)
            {
                case null:
                case BackupKind.None:
                    return null; // do not optimize for Import
                case BackupKind.Full:
                case BackupKind.Incremental:
                    return CreateCompareExchangeActions(databaseName, context, backupKind);
                default:
                    throw new ArgumentOutOfRangeException(nameof(backupKind), backupKind, null);
            }
        }

        protected virtual ICompareExchangeActions CreateCompareExchangeActions(string databaseName, JsonOperationContext context, BackupKind? backupKind)
        {
            return new DatabaseCompareExchangeActions(databaseName, _database, context, backupKind, _result, _onProgress, _token);
        }

        public ICompareExchangeActions CompareExchangeTombstones(string databaseName, JsonOperationContext context)
        {
            return new DatabaseCompareExchangeActions(databaseName, _database, context, backupKind: null, _result, _onProgress, _token);
        }

        public ICounterActions Counters(SmugglerResult result)
        {
            return new CounterActions(_database, result);
        }

        public virtual ISubscriptionActions Subscriptions()
        {
            return new DatabaseSubscriptionActions(_database.ServerStore, _database.Name);
        }

        public IReplicationHubCertificateActions ReplicationHubCertificates()
        {
            return new DatabaseReplicationHubCertificateActions(_database);
        }

        public ITimeSeriesActions TimeSeries()
        {
            return new TimeSeriesActions(_database);
        }

        public ITimeSeriesActions TimeSeriesDeletedRanges()
        {
            return new TimeSeriesActions(_database);
        }

        public ILegacyActions LegacyDocumentDeletions()
        {
            // Used only in Stream Destination, needed when we writing from Stream Source to Stream Destination
            throw new NotSupportedException("LegacyDocumentDeletions is not supported in Database destination, " +
                                            "it is only supported when writing to Stream destination. Shouldn't happen.");
        }

        public ILegacyActions LegacyAttachmentDeletions()
        {
            // Used only in Stream Destination, needed when we writing from Stream Source to Stream Destination
            throw new NotSupportedException("LegacyAttachmentDeletions is not supported in Database destination, " +
                                            "it is only supported when writing to Stream destination. Shouldn't happen.");
        }

        public IIndexActions Indexes()
        {
            return new DatabaseIndexActions(_database.IndexStore.Create, _database.Time);
        }

        public sealed class DuplicateDocsHandler : IDisposable
        {
            private readonly DocumentDatabase _database;
            private DocumentsOperationContext _context;
            private IDisposable _returnContext;

            public List<DocumentItem> DocumentsWithDuplicateCollection;
            internal bool _markForDispose;

            public DuplicateDocsHandler(DocumentDatabase database)
            {
                _database = database;
            }

            private void InitializeIfNeeded()
            {
                _returnContext ??= _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
                DocumentsWithDuplicateCollection ??= new List<DocumentItem>();
            }

            public void AddDocument(DocumentItem item)
            {
                InitializeIfNeeded();

                DocumentsWithDuplicateCollection.Add(new DocumentItem
                {
                    Document = item.Document.Clone(_context)
                });
            }

            public void Dispose()
            {
                _returnContext?.Dispose();
                _returnContext = null;
            }
        }

        public sealed class DatabaseDocumentActions : IDocumentActions
        {
            private readonly DocumentDatabase _database;
            private readonly BuildVersionType _buildType;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly bool _isRevision;
            private readonly Logger _log;
            private MergedBatchPutCommand _command;
            private MergedBatchPutCommand _prevCommand;
            private Task _prevCommandTask = Task.CompletedTask;

            private MergedBatchDeleteRevisionCommand _revisionDeleteCommand;
            private MergedBatchDeleteRevisionCommand _prevRevisionDeleteCommand;
            private Task _prevRevisionCommandTask = Task.CompletedTask;

            private MergedBatchFixDocumentMetadataCommand _fixDocumentMetadataCommand;
            private MergedBatchFixDocumentMetadataCommand _prevFixDocumentMetadataCommand;
            private Task _prevFixDocumentMetadataCommandTask = Task.CompletedTask;

            private readonly Sparrow.Size _enqueueThreshold;
            private readonly ConcurrentDictionary<string, CollectionName> _missingDocumentsForRevisions;
            private readonly HashSet<string> _documentIdsOfMissingAttachments;
            private readonly DuplicateDocsHandler _duplicateDocsHandler;
            private readonly bool _throwOnCollectionMismatchError;

            public DatabaseDocumentActions(DocumentDatabase database, BuildVersionType buildType, DatabaseSmugglerOptionsServerSide options, bool isRevision, Logger log, DuplicateDocsHandler duplicateDocsHandler, bool throwOnCollectionMismatchError)
            {
                _database = database;
                _buildType = buildType;
                _options = options;
                _isRevision = isRevision;
                _log = log;
                _enqueueThreshold = new Size(database.Is32Bits ? 2 : 32, SizeUnit.Megabytes);
                _duplicateDocsHandler = duplicateDocsHandler;
                _throwOnCollectionMismatchError = throwOnCollectionMismatchError;

                _missingDocumentsForRevisions = isRevision || buildType == BuildVersionType.V3 ? new ConcurrentDictionary<string, CollectionName>() : null;
                _documentIdsOfMissingAttachments = isRevision ? null : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                _command = new MergedBatchPutCommand(database, buildType, log, _missingDocumentsForRevisions, _documentIdsOfMissingAttachments)
                {
                    IsRevision = isRevision,
                };

                if (_throwOnCollectionMismatchError == false)
                    _command.DocumentCollectionMismatchHandler = item => _duplicateDocsHandler.AddDocument(item);
            }

            public ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress, Func<ValueTask> beforeFlushing)
            {
                if (item.Attachments != null)
                {
                    if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false)
                        progress.Attachments.Skipped = true;
                }

                _command.Add(item);
                return HandleBatchOfDocumentsIfNecessaryAsync(beforeFlushing);
            }

            public async ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                _command.Add(new DocumentItem
                {
                    Tombstone = tombstone
                });
                await HandleBatchOfDocumentsIfNecessaryAsync(null);
            }

            public async ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                _command.Add(new DocumentItem
                {
                    Conflict = conflict
                });
                await HandleBatchOfDocumentsIfNecessaryAsync(null);
            }

            public async ValueTask DeleteDocumentAsync(string id)
            {
                await _database.TxMerger.Enqueue(new DeleteDocumentCommand(id, null, _database));
            }

            public IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection()
            {
                if (_duplicateDocsHandler.DocumentsWithDuplicateCollection == null)
                    yield break;

                if (_duplicateDocsHandler.DocumentsWithDuplicateCollection.Count == 0)
                    yield break;

                foreach (var item in _duplicateDocsHandler.DocumentsWithDuplicateCollection)
                {
                    yield return item;
                }

                _duplicateDocsHandler._markForDispose = true;
            }

            public Task<Stream> GetTempStreamAsync()
            {
                if (_command.AttachmentStreamsTempFile == null)
                    _command.AttachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("smuggler");

                return Task.FromResult(_command.AttachmentStreamsTempFile.StartNewStream());
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _command.Context.CachedProperties.NewDocument();
                return _command.Context;
            }

            public BlittableJsonDocumentBuilder GetBuilderForNewDocument(UnmanagedJsonParser parser, JsonParserState state, BlittableMetadataModifier modifier = null)
            {
                return _command.GetOrCreateBuilder(parser, state, "import/object", modifier);
            }

            public BlittableMetadataModifier GetMetadataModifierForNewDocument(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                return _command.GetOrCreateMetadataModifier(firstEtagOfLegacyRevision, legacyRevisionsCount, legacyImport, readLegacyEtag, operateOnTypes);
            }

            public async ValueTask DisposeAsync()
            {
                await FinishBatchOfDocumentsAsync();
                await FixDocumentMetadataIfNecessaryAsync();
                await DeleteRevisionsForNonExistingDocumentsAsync();

                if (_duplicateDocsHandler._markForDispose)
                    _duplicateDocsHandler.Dispose();
            }

            private async ValueTask FixDocumentMetadataIfNecessaryAsync()
            {
                if (_documentIdsOfMissingAttachments == null ||
                    _documentIdsOfMissingAttachments.Count == 0)
                    return;

                _fixDocumentMetadataCommand = new MergedBatchFixDocumentMetadataCommand(_database, _log);

                foreach (var docId in _documentIdsOfMissingAttachments)
                {
                    _fixDocumentMetadataCommand.Add(docId);
                    await HandleBatchOfFixDocumentsMetadataIfNecessaryAsync();
                }

                await FinishBatchOfFixDocumentsMetadataAsync();
            }

            private async ValueTask HandleBatchOfFixDocumentsMetadataIfNecessaryAsync()
            {
                var prevDoneAndHasEnough = _fixDocumentMetadataCommand.Context.AllocatedMemory > Constants.Size.Megabyte && _prevRevisionCommandTask.IsCompleted;
                var currentReachedLimit = _fixDocumentMetadataCommand.Context.AllocatedMemory > _enqueueThreshold.GetValue(SizeUnit.Bytes);

                if (currentReachedLimit == false && prevDoneAndHasEnough == false)
                    return;

                var prevCommand = _prevFixDocumentMetadataCommand;
                var prevCommandTask = _prevFixDocumentMetadataCommandTask;
                var commandTask = _database.TxMerger.Enqueue(_fixDocumentMetadataCommand);
                // we ensure that we first enqueue the command to if we
                // fail to do that, we won't be waiting on the previous
                // one
                _prevFixDocumentMetadataCommand = _fixDocumentMetadataCommand;
                _prevFixDocumentMetadataCommandTask = commandTask;

                if (prevCommand != null)
                {
                    using (prevCommand)
                    {
                        await prevCommandTask;
                        Debug.Assert(prevCommand.IsDisposed == false,
                            "we rely on reusing this context on the next batch, so it has to be disposed here");
                    }
                }

                _fixDocumentMetadataCommand = new MergedBatchFixDocumentMetadataCommand(_database, _log);
            }

            private async ValueTask FinishBatchOfFixDocumentsMetadataAsync()
            {
                if (_prevFixDocumentMetadataCommand != null)
                {
                    using (_prevFixDocumentMetadataCommand)
                        await _prevFixDocumentMetadataCommandTask;

                    _prevFixDocumentMetadataCommand = null;
                }

                using (_fixDocumentMetadataCommand)
                {
                    if (_fixDocumentMetadataCommand.Ids.Count > 0)
                    {
                        await _database.TxMerger.Enqueue(_fixDocumentMetadataCommand);
                    }
                }

                _fixDocumentMetadataCommand = null;
            }

            private async ValueTask DeleteRevisionsForNonExistingDocumentsAsync()
            {
                if (_missingDocumentsForRevisions == null ||
                    _missingDocumentsForRevisions.Count == 0)
                    return;

                _revisionDeleteCommand = new MergedBatchDeleteRevisionCommand(_database, _log);

                foreach (var docId in _missingDocumentsForRevisions)
                {
                    _revisionDeleteCommand.Add(docId);
                    await HandleBatchOfRevisionsIfNecessaryAsync();
                }

                await FinishBatchOfRevisionsAsync();
            }

            private async ValueTask HandleBatchOfRevisionsIfNecessaryAsync()
            {
                var prevDoneAndHasEnough = _revisionDeleteCommand.Context.AllocatedMemory > Constants.Size.Megabyte && _prevRevisionCommandTask.IsCompleted;
                var currentReachedLimit = _revisionDeleteCommand.Context.AllocatedMemory > _enqueueThreshold.GetValue(SizeUnit.Bytes);

                if (currentReachedLimit == false && prevDoneAndHasEnough == false)
                    return;

                var prevCommand = _prevRevisionDeleteCommand;
                var prevCommandTask = _prevRevisionCommandTask;
                var commandTask = _database.TxMerger.Enqueue(_revisionDeleteCommand);
                // we ensure that we first enqueue the command to if we
                // fail to do that, we won't be waiting on the previous
                // one
                _prevRevisionDeleteCommand = _revisionDeleteCommand;
                _prevRevisionCommandTask = commandTask;

                if (prevCommand != null)
                {
                    using (prevCommand)
                    {
                        await prevCommandTask;
                        Debug.Assert(prevCommand.IsDisposed == false,
                            "we rely on reusing this context on the next batch, so it has to be disposed here");
                    }
                }

                _revisionDeleteCommand = new MergedBatchDeleteRevisionCommand(_database, _log);
            }

            private async ValueTask FinishBatchOfRevisionsAsync()
            {
                if (_prevRevisionDeleteCommand != null)
                {
                    using (_prevRevisionDeleteCommand)
                        await _prevRevisionCommandTask;

                    _prevRevisionDeleteCommand = null;
                }

                using (_revisionDeleteCommand)
                {
                    if (_revisionDeleteCommand.Ids.Count > 0)
                    {
                        await _database.TxMerger.Enqueue(_revisionDeleteCommand);
                    }
                }

                _revisionDeleteCommand = null;
            }

            private ValueTask HandleBatchOfDocumentsIfNecessaryAsync(Func<ValueTask> beforeFlush)
            {
                var commandSize = _command.GetCommandAllocationSize();
                var prevDoneAndHasEnough = commandSize > Constants.Size.Megabyte && _prevCommandTask.IsCompleted;
                var currentReachedLimit = commandSize > _enqueueThreshold.GetValue(SizeUnit.Bytes);

                if (currentReachedLimit == false && prevDoneAndHasEnough == false)
                {
                    return ValueTask.CompletedTask;
                }

                return new ValueTask(SubmitNextBatch());

                async Task SubmitNextBatch()
                {
                    if (beforeFlush != null)
                        await beforeFlush();
                    var prevCommand = _prevCommand;
                    var prevCommandTask = _prevCommandTask;

                    var commandTask = _database.TxMerger.Enqueue(_command);
                    // we ensure that we first enqueue the command to if we
                    // fail to do that, we won't be waiting on the previous
                    // one
                    _prevCommand = _command;
                    _prevCommandTask = commandTask;

                    if (prevCommand != null)
                    {
                        using (prevCommand)
                        {
                            await prevCommandTask;
                            Debug.Assert(prevCommand.IsDisposed == false,
                                "we rely on reusing this context on the next batch, so it has to be disposed here");
                        }
                    }

                    _command = new MergedBatchPutCommand(_database, _buildType, _log,
                            _missingDocumentsForRevisions, _documentIdsOfMissingAttachments)
                    { IsRevision = _isRevision, };

                    if (_throwOnCollectionMismatchError == false)
                        _command.DocumentCollectionMismatchHandler = item => _duplicateDocsHandler.AddDocument(item);
                }
            }

            private async ValueTask FinishBatchOfDocumentsAsync()
            {
                if (_prevCommand != null)
                {
                    using (_prevCommand)
                        await _prevCommandTask;

                    _prevCommand = null;
                }

                using (_command)
                {
                    if (_command.Documents.Count > 0)
                        await _database.TxMerger.Enqueue(_command);
                }

                _command = null;
            }
        }

        public sealed class MergedBatchPutCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
        {
            public bool IsRevision;
            public Action<DocumentItem> DocumentCollectionMismatchHandler;

            private readonly DocumentDatabase _database;
            private readonly BuildVersionType _buildType;
            private readonly Logger _log;

            public readonly List<DocumentItem> Documents = new List<DocumentItem>();
            public StreamsTempFile AttachmentStreamsTempFile;

            private IDisposable _resetContext;
            private bool _isDisposed;

            public bool IsDisposed => _isDisposed;
            private readonly ConcurrentDictionary<string, CollectionName> _missingDocumentsForRevisions;
            private readonly HashSet<string> _documentIdsOfMissingAttachments;
            private readonly DocumentsOperationContext _context;
            private long _attachmentsStreamSizeOverhead;

            private BlittableJsonDocumentBuilder _builder;
            private BlittableMetadataModifier _metadataModifier;

            public MergedBatchPutCommand(DocumentDatabase database, BuildVersionType buildType,
                Logger log,
                ConcurrentDictionary<string, CollectionName> missingDocumentsForRevisions = null,
                HashSet<string> documentIdsOfMissingAttachments = null)
            {
                _database = database;
                _buildType = buildType;
                _log = log;
                _resetContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
                _missingDocumentsForRevisions = missingDocumentsForRevisions;
                _documentIdsOfMissingAttachments = documentIdsOfMissingAttachments;

                if (_database.Is32Bits)
                {
                    using (var ctx = DocumentsOperationContext.ShortTermSingleUse(database))
                    using (ctx.OpenReadTransaction())
                    {
                        _collectionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        foreach (var collection in _database.DocumentsStorage.GetCollections(ctx))
                        {
                            _collectionNames.Add(collection.Name);
                        }
                    }
                }
            }

            public DocumentsOperationContext Context => _context;

            public BlittableJsonDocumentBuilder GetOrCreateBuilder(UnmanagedJsonParser parser, JsonParserState state, string debugTag, BlittableMetadataModifier modifier = null)
            {
                return _builder ??= new BlittableJsonDocumentBuilder(_context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, debugTag, parser, state, modifier: modifier);
            }

            public BlittableMetadataModifier GetOrCreateMetadataModifier(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                _metadataModifier ??= new BlittableMetadataModifier(_context, legacyImport, readLegacyEtag, operateOnTypes);
                _metadataModifier.FirstEtagOfLegacyRevision = firstEtagOfLegacyRevision;
                _metadataModifier.LegacyRevisionsCount = legacyRevisionsCount;

                return _metadataModifier;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Importing {Documents.Count:#,#0} documents");

                var idsOfDocumentsToUpdateAfterAttachmentDeletion = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var documentType in Documents)
                {
                    var tombstone = documentType.Tombstone;
                    long newEtag;
                    if (tombstone != null)
                    {
                        using (Slice.External(context.Allocator, tombstone.LowerId, out Slice key))
                        {
                            newEtag = _database.DocumentsStorage.GenerateNextEtag();
                            tombstone.ChangeVector = _database.DocumentsStorage.GetNewChangeVector(context, newEtag);

                            switch (tombstone.Type)
                            {
                                case Tombstone.TombstoneType.Document:
                                    AddTrxnIfNeeded(context, tombstone.LowerId, ref tombstone.ChangeVector);
                                    _database.DocumentsStorage.Delete(context, key, tombstone.LowerId, null, tombstone.LastModified.Ticks, context.GetChangeVector(tombstone.ChangeVector), new CollectionName(tombstone.Collection), newFlags: tombstone.Flags);
                                    break;

                                case Tombstone.TombstoneType.Attachment:
                                    var idEnd = key.Content.IndexOf(SpecialChars.RecordSeparator);
                                    if (idEnd < 1)
                                        throw new InvalidOperationException("Cannot find a document ID inside the attachment key");
                                    var attachmentId = key.Content.Substring(idEnd);
                                    idsOfDocumentsToUpdateAfterAttachmentDeletion.Add(attachmentId);
                                    string collection;
                                    using (var doc1 = context.DocumentDatabase.DocumentsStorage.Get(context, attachmentId, DocumentFields.Default, throwOnConflict: false))
                                    {
                                        doc1.TryGetCollection(out collection);
                                    }
                                    _database.DocumentsStorage.AttachmentsStorage.DeleteAttachmentDirect(context, key, false, "$fromReplication", null, tombstone.ChangeVector, tombstone.LastModified.Ticks, collection);
                                    break;

                                case Tombstone.TombstoneType.Revision:
                                    using (RevisionTombstoneReplicationItem.TryExtractChangeVectorSliceFromKey(context.Allocator, tombstone.LowerId, out var changeVectorSlice))
                                    {
                                        _database.DocumentsStorage.RevisionsStorage.DeleteRevision(context, key, tombstone.Collection, tombstone.ChangeVector, tombstone.LastModified.Ticks, changeVectorSlice, fromReplication: false);
                                    }
                                    break;

                                case Tombstone.TombstoneType.Counter:
                                    _database.DocumentsStorage.CountersStorage.DeleteCounter(context, key.ToString(), tombstone.Collection, null);
                                    break;
                            }
                        }

                        continue;
                    }

                    var conflict = documentType.Conflict;
                    if (conflict != null)
                    {
                        ChangeVector.MergeWithDatabaseChangeVector(context, documentType.Conflict.ChangeVector);
                        _database.DocumentsStorage.ConflictsStorage.AddConflict(context, conflict.Id, conflict.LastModified.Ticks, conflict.Doc, conflict.ChangeVector,
                            conflict.Collection, conflict.Flags, NonPersistentDocumentFlags.FromSmuggler);

                        continue;
                    }

                    if (documentType.Attachments != null)
                    {
                        foreach (var attachment in documentType.Attachments)
                        {
                            _database.DocumentsStorage.AttachmentsStorage.PutAttachmentStream(context, attachment.Tag, attachment.Base64Hash, attachment.Stream);
                        }
                    }

                    var document = documentType.Document;
                    var id = document.Id;
                    var documentChangeVector = context.GetChangeVector(document.ChangeVector);

                    if (IsRevision)
                    {
                        PutAttachments(context, document, isRevision: true, out _);

                        if ((document.NonPersistentFlags.Contain(NonPersistentDocumentFlags.FromSmuggler)) &&
                            (_missingDocumentsForRevisions != null))
                        {
                            if (_database.DocumentsStorage.Get(context, document.Id) == null &&
                                document.ChangeVector.Contains(ChangeVectorParser.TrxnTag) == false)
                            {
                                var collection = _database.DocumentsStorage.ExtractCollectionName(context, document.Data);
                                _missingDocumentsForRevisions.TryAdd(document.Id.ToString(), collection);
                            }
                        }

                        if (document.Flags.Contain(DocumentFlags.DeleteRevision))
                        {
                            _missingDocumentsForRevisions?.TryRemove(id, out _);
                            _database.DocumentsStorage.RevisionsStorage.Delete(context, id, document.Data, document.Flags,
                                document.NonPersistentFlags, documentChangeVector, document.LastModified.Ticks);
                        }
                        else
                        {
                            _database.DocumentsStorage.RevisionsStorage.Put(context, id, document.Data, document.Flags,
                                document.NonPersistentFlags, documentChangeVector, document.LastModified.Ticks);
                        }

                        ChangeVector.MergeWithDatabaseChangeVector(context, documentChangeVector);

                        continue;
                    }

                    if (DatabaseSmuggler.IsPreV4Revision(_buildType, id, document))
                    {
                        // handle old revisions
                        if (_database.DocumentsStorage.RevisionsStorage.Configuration == null)
                            ThrowRevisionsDisabled();

                        var endIndex = id.IndexOf(DatabaseSmuggler.PreV4RevisionsDocumentId, StringComparison.OrdinalIgnoreCase);
                        var newId = id.Substring(0, endIndex);

                        Document parentDocument = null;
                        if (_database.DocumentsStorage.Get(context, newId, DocumentFields.Id) == null)
                        {
                            var collection = _database.DocumentsStorage.ExtractCollectionName(context, document.Data);
                            _missingDocumentsForRevisions.TryAdd(newId, collection);
                        }
                        else
                        {
                            // the order of revisions in v3.x is different than we have in v4.x
                            // in v4.x: rev1, rev2, rev3, document (the change vector of the last revision is identical to the document)
                            // in v3.x: rev1, rev2, document, rev3
                            parentDocument = _database.DocumentsStorage.Get(context, newId);
                            _missingDocumentsForRevisions.TryRemove(newId, out _);
                            documentChangeVector = documentChangeVector.MergeWith(parentDocument.ChangeVector, context);
                        }

                        document.Flags |= DocumentFlags.HasRevisions;
                        _database.DocumentsStorage.RevisionsStorage.Put(context, newId, document.Data, document.Flags,
                            document.NonPersistentFlags, documentChangeVector, document.LastModified.Ticks);

                        if (parentDocument != null)
                        {
                            // the change vector of the document must be identical to the one of the last revision
                            ChangeVector.MergeWithDatabaseChangeVector(context, documentChangeVector);

                            using (parentDocument.Data)
                                parentDocument.Data = parentDocument.Data.Clone(context);

                            _database.DocumentsStorage.Put(context, parentDocument.Id, null,
                                parentDocument.Data, parentDocument.LastModified.Ticks, documentChangeVector, null,
                                parentDocument.Flags, parentDocument.NonPersistentFlags);
                        }

                        continue;
                    }

                    PutAttachments(context, document, isRevision: false, out bool updateDocumentMetadata);
                    if (updateDocumentMetadata)
                        document.NonPersistentFlags |= NonPersistentDocumentFlags.ResolveAttachmentsConflict;

                    newEtag = _database.DocumentsStorage.GenerateNextEtag();
                    document.ChangeVector = _database.DocumentsStorage.GetNewChangeVector(context, newEtag);

                    AddTrxnIfNeeded(context, id, ref document.ChangeVector);

                    try
                    {
                        _database.DocumentsStorage.Put(context, id, expectedChangeVector: null, document.Data, document.LastModified.Ticks, document.ChangeVector, null, document.Flags, document.NonPersistentFlags);
                    }
                    catch (DocumentCollectionMismatchException)
                    {
                        if (DocumentCollectionMismatchHandler == null)
                            throw;

                        DocumentCollectionMismatchHandler.Invoke(documentType);
                    }
                }

                foreach (var idToUpdate in idsOfDocumentsToUpdateAfterAttachmentDeletion)
                {
                    _database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, idToUpdate);
                }

                return Documents.Count;
            }

            private void AddTrxnIfNeeded(DocumentsOperationContext context, string id, ref string changeVector)
            {
                using (var doc = _database.DocumentsStorage.Get(context, id, DocumentFields.ChangeVector))
                {
                    string oldChangeVector;

                    if (doc != null)
                    {
                        oldChangeVector = doc.ChangeVector;
                    }
                    else
                    {
                        using (var tombstone = _database.DocumentsStorage.GetDocumentOrTombstone(context, id).Tombstone)
                        {
                            oldChangeVector = tombstone?.ChangeVector;
                        }
                    }

                    //The ClusterTransactionId can be null if the database was migrated from version smaller then v5.2 
                    if (_database.ClusterTransactionId != null)
                    {
                        var trxn = ChangeVectorUtils.GetEtagById(oldChangeVector, _database.ClusterTransactionId);
                        if (trxn > 0)
                            changeVector += $",TRXN:{trxn}-{_database.ClusterTransactionId}";
                    }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private unsafe void PutAttachments(DocumentsOperationContext context, Document document, bool isRevision, out bool hasAttachments)
            {
                hasAttachments = false;

                if (document.Data.TryGet(Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Client.Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                var type = (document.Flags & DocumentFlags.Revision) == DocumentFlags.Revision ? AttachmentType.Revision : AttachmentType.Document;
                var attachmentsStorage = _database.DocumentsStorage.AttachmentsStorage;
                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    hasAttachments = true;
                    if (attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false ||
                        attachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue contentType) == false ||
                        attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false ||
                        attachment.TryGet(nameof(AttachmentName.Flags), out AttachmentFlags flags) == false ||
                        attachment.TryGet(nameof(AttachmentName.Size), out long size) == false ||
                        attachment.TryGet(nameof(AttachmentName.RetireAt), out DateTime? retireAt) == false)

                        throw new ArgumentException($"The attachment info is missing a mandatory value: {attachment}");

                    if (isRevision == false)
                    {
                        if (flags == AttachmentFlags.None && attachmentsStorage.AttachmentExists(context, hash) == false)
                            _documentIdsOfMissingAttachments.Add(document.Id);
                        CollectionName collectionName = _database.DocumentsStorage.ExtractCollectionName(context, document.Data);
                        attachmentsStorage.PutAttachment(context, document.Id, name, contentType, hash, flags, size, retireAt, updateDocument: false, fromSmuggler: true, collection2: collectionName);
                        continue;
                    }

                    using (DocumentIdWorker.GetSliceFromId(_context, document.Id, out Slice lowerDocumentId))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(_context, name, out Slice lowerName, out Slice nameSlice))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(_context, contentType, out Slice lowerContentType, out Slice contentTypeSlice))
                    using (Slice.External(_context.Allocator, hash, out Slice base64Hash))
                    using (Slice.From(_context.Allocator, document.ChangeVector, out Slice cv))
                    using (attachmentsStorage.GetAttachmentKey(_context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size,
                               base64Hash, lowerContentType.Content.Ptr, lowerContentType.Size, type, cv, out Slice keySlice))
                    {
                        attachmentsStorage.PutDirect(context, keySlice, nameSlice, contentTypeSlice, base64Hash, retireAt, flags, size, isRevision: true);
                    }
                }
            }

            [DoesNotReturn]
            private static void ThrowRevisionsDisabled()
            {
                throw new InvalidOperationException("Revisions needs to be enabled before import!");
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;

                foreach (var doc in Documents)
                {
                    if (doc.Document != null)
                    {
                        doc.Document.Data.Dispose();

                        if (doc.Attachments != null)
                        {
                            foreach (var attachment in doc.Attachments)
                            {
                                attachment.Dispose();
                            }
                        }
                    }
                }
                Documents.Clear();

                _metadataModifier?.Dispose();
                _metadataModifier = null;

                _builder?.Dispose();
                _builder = null;

                _resetContext?.Dispose();
                _resetContext = null;

                AttachmentStreamsTempFile?.Dispose();
                AttachmentStreamsTempFile = null;
            }

            /// <summary>
            /// Return the actual size this command allocates including the stream sizes
            /// </summary>
            /// <returns></returns>
            public long GetCommandAllocationSize()
            {
                return Context.AllocatedMemory + _attachmentsStreamSizeOverhead + _schemaOverHeadSize;
            }

            private HashSet<string> _collectionNames;
            private int _schemaOverHeadSize;

            public void Add(DocumentItem document)
            {
                Documents.Add(document);
                if (document.Attachments != null)
                {
                    if (document.Document.TryGetMetadata(out var metadata)
                        && metadata.TryGet(Client.Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments))
                    {
                        foreach (BlittableJsonReaderObject attachment in attachments)
                        {
                            if (attachment.TryGet(nameof(Attachment.Size), out long size))
                            {
                                _attachmentsStreamSizeOverhead += size;
                            }
                        }
                    }
                }

                if (_database.Is32Bits && document.Document != null)
                {
                    if (document.Document.TryGetMetadata(out var metadata)
                        && metadata.TryGet(Client.Constants.Documents.Metadata.Collection, out string collectionName)
                        && _collectionNames.Add(collectionName))
                    {
                        _schemaOverHeadSize += SchemaSize;
                    }
                }
            }

            private const int SchemaSize = 2 * 1024 * 1024;

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new MergedBatchPutCommandDto
                {
                    BuildType = _buildType,
                    Documents = Documents,
                    IsRevision = IsRevision
                };
            }
        }

        public sealed class MergedBatchPutCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedBatchPutCommand>
        {
            public BuildVersionType BuildType;
            public List<DocumentItem> Documents;
            public bool IsRevision;

            public MergedBatchPutCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
                var command = new MergedBatchPutCommand(database, BuildType, log)
                {
                    IsRevision = IsRevision
                };
                foreach (var document in Documents)
                {
                    command.Add(document);
                }

                return command;
            }
        }

        internal sealed class MergedBatchFixDocumentMetadataCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
        {
            private readonly Logger _log;
            public HashSet<string> Ids = new HashSet<string>();
            private readonly DocumentDatabase _database;
            private readonly DocumentsOperationContext _context;
            public DocumentsOperationContext Context => _context;
            private bool _isDisposed;
            private readonly IDisposable _returnContext;
            public bool IsDisposed => _isDisposed;

            public MergedBatchFixDocumentMetadataCommand(DocumentDatabase database, Logger log)
            {
                _database = database;
                _log = log;
                _returnContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Trying to update {Ids.Count:#,#0} documents metadata if necessary");

                var count = 0;
                foreach (var id in Ids)
                {
                    using (DocumentIdWorker.GetSliceFromId(context, id, out var lowerId))
                    {
                        var document = _database.DocumentsStorage.Get(context, lowerId, throwOnConflict: false, skipValidationInDebug: true);
                        if (document == null)
                            continue;

                        if (document.Data.TryGet(Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                            metadata.TryGet(Client.Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                            continue;

                        var attachmentsToRemoveNames = new HashSet<LazyStringValue>();
                        var attachmentsToRemoveHashes = new HashSet<LazyStringValue>();

                        foreach (BlittableJsonReaderObject attachment in attachments)
                        {
                            if (attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false ||
                                attachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue _) == false ||
                                attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                                throw new ArgumentException($"The attachment info in missing a mandatory value: {attachment}");

                            var attachmentsStorage = _database.DocumentsStorage.AttachmentsStorage;
                            if (attachmentsStorage.AttachmentExists(context, hash) == false)
                            {
                                attachmentsToRemoveNames.Add(name);
                                attachmentsToRemoveHashes.Add(hash);
                            }
                        }

                        if (attachmentsToRemoveNames.Count == 0)
                            continue;

                        count++;
                        var attachmentsToSave = new DynamicJsonArray();

                        foreach (BlittableJsonReaderObject attachment in attachments)
                        {
                            attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash);

                            if (attachmentsToRemoveHashes.Contains(hash))
                                continue;

                            attachmentsToSave.Add(attachment);
                        }

                        foreach (var toRemove in attachmentsToRemoveNames)
                        {
                            _database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, id, toRemove, null, collectionName: out _, updateDocument: false, extractCollectionName: false);
                        }

                        metadata.Modifications = new DynamicJsonValue(metadata);
                        document.Data.Modifications = new DynamicJsonValue(document.Data)
                        {
                            [Client.Constants.Documents.Metadata.Key] = metadata
                        };

                        if (attachmentsToSave.Count == 0)
                        {
                            document.Flags = document.Flags.Strip(DocumentFlags.HasAttachments);
                            metadata.Modifications.Remove(Client.Constants.Documents.Metadata.Attachments);
                        }
                        else
                        {
                            document.Flags |= DocumentFlags.HasAttachments;
                            metadata.Modifications = new DynamicJsonValue(metadata)
                            {
                                [Client.Constants.Documents.Metadata.Attachments] = attachmentsToSave
                            };
                        }

                        using (var old = document.Data)
                        {
                            var newDocument = context.ReadObject(old, document.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            _database.DocumentsStorage.Put(context, document.Id, null, newDocument);
                        }
                    }
                }

                if (_log.IsInfoEnabled)
                    _log.Info($"Updated {count:#,#0} documents metadata");

                return count;
            }

            public void Add(string id)
            {
                Ids.Add(id);
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;
                Ids.Clear();
                _returnContext.Dispose();
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new MergedBatchFixDocumentMetadataCommandDto
                {
                    Ids = Ids
                };
            }

            internal sealed class MergedBatchFixDocumentMetadataCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedBatchFixDocumentMetadataCommand>
            {
                public HashSet<string> Ids = new HashSet<string>();

                public MergedBatchFixDocumentMetadataCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    var log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
                    var command = new MergedBatchFixDocumentMetadataCommand(database, log);

                    foreach (var id in Ids)
                    {
                        command.Add(id);
                    }

                    return command;
                }
            }
        }

        internal sealed class MergedBatchDeleteRevisionCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
        {
            private readonly Logger _log;
            public readonly List<KeyValuePair<string, CollectionName>> Ids = new List<KeyValuePair<string, CollectionName>>();
            private readonly DocumentDatabase _database;
            private readonly DocumentsOperationContext _context;
            public DocumentsOperationContext Context => _context;
            private bool _isDisposed;
            private readonly IDisposable _returnContext;
            public bool IsDisposed => _isDisposed;

            public MergedBatchDeleteRevisionCommand(DocumentDatabase database, Logger log)
            {
                _database = database;
                _log = log;
                _returnContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Deleting {Ids.Count:#,#0} revisions");

                foreach (var id in Ids)
                {
                    using (DocumentIdWorker.GetSliceFromId(context, id.Key, out var lowerId))
                    {
                        _database.DocumentsStorage.RevisionsStorage.Delete(context,
                            id.Key,
                            lowerId,
                            id.Value,
                            _database.DocumentsStorage.GetNewChangeVector(context, _database.DocumentsStorage.GenerateNextEtag()),
                            _database.Time.GetUtcNow().Ticks,
                            NonPersistentDocumentFlags.FromSmuggler,
                            DocumentFlags.DeleteRevision);
                    }
                }
                return 1;
            }

            public void Add(KeyValuePair<string, CollectionName> id)
            {
                Ids.Add(id);
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;
                Ids.Clear();
                _returnContext.Dispose();
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new MergedBatchDeleteRevisionCommandDto
                {
                    Ids = Ids
                };
            }
        }

        internal sealed class MergedBatchDeleteRevisionCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedBatchDeleteRevisionCommand>
        {
            public List<KeyValuePair<string, CollectionName>> Ids = new List<KeyValuePair<string, CollectionName>>();

            public MergedBatchDeleteRevisionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
                var command = new MergedBatchDeleteRevisionCommand(database, log);

                foreach (var id in Ids)
                {
                    command.Add(id);
                }

                return command;
            }
        }

        private sealed class CounterActions : ICounterActions
        {
            private readonly DocumentDatabase _database;
            private CountersHandler.SmugglerCounterBatchCommand _cmd;
            private CountersHandler.SmugglerCounterBatchCommand _prevCommand;
            private Task _prevCommandTask = Task.CompletedTask;
            private int _countersCount;
            private readonly int _maxBatchSize;

            private SmugglerResult _result;

            public CounterActions(DocumentDatabase database, SmugglerResult result)
            {
                _database = database;
                _result = result;
                _cmd = new CountersHandler.SmugglerCounterBatchCommand(_database, _result);

                _maxBatchSize = _database.Is32Bits ? 2 * 1024 : 10 * 1024;
            }

            private void AddToBatch(CounterGroupDetail counterGroupDetail)
            {
                _cmd.Add(counterGroupDetail);

                counterGroupDetail.Values.TryGet(CountersStorage.Values, out BlittableJsonReaderObject counters);
                _countersCount += counters?.Count ?? 0;
            }

            private void AddToBatch(CounterDetail counter)
            {
                _cmd.AddLegacy(counter.DocumentId, counter);
                _countersCount++;
            }

            public async ValueTask WriteCounterAsync(CounterGroupDetail counterDetail)
            {
                AddToBatch(counterDetail);
                await HandleBatchOfCountersIfNecessaryAsync();
            }

            public async ValueTask WriteLegacyCounterAsync(CounterDetail counterDetail)
            {
                AddToBatch(counterDetail);
                await HandleBatchOfCountersIfNecessaryAsync();
            }

            public void RegisterForDisposal(IDisposable data)
            {
                _cmd.RegisterForDisposal(data);
            }

            public async ValueTask DisposeAsync()
            {
                await FinishBatchOfCountersAsync();
            }

            private async ValueTask HandleBatchOfCountersIfNecessaryAsync()
            {
                if (_countersCount < _maxBatchSize)
                    return;

                var prevCommand = _prevCommand;
                var prevCommandTask = _prevCommandTask;

                var commandTask = _database.TxMerger.Enqueue(_cmd);

                _prevCommand = _cmd;
                _prevCommandTask = commandTask;

                if (prevCommand != null)
                {
                    using (prevCommand)
                    {
                        await prevCommandTask;
                    }
                }

                _cmd = new CountersHandler.SmugglerCounterBatchCommand(_database, _result);

                _countersCount = 0;
            }

            private async ValueTask FinishBatchOfCountersAsync()
            {
                if (_prevCommand != null)
                {
                    using (_prevCommand)
                    {
                        await _prevCommandTask;
                    }

                    _prevCommand = null;
                }

                using (_cmd)
                {
                    if (_countersCount > 0)
                    {
                        await _database.TxMerger.Enqueue(_cmd);
                    }
                }

                _cmd = null;
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _cmd.Context.CachedProperties.NewDocument();
                return _cmd.Context;
            }

            public BlittableJsonDocumentBuilder GetBuilderForNewDocument(UnmanagedJsonParser parser, JsonParserState state, BlittableMetadataModifier modifier = null)
            {
                return _cmd.GetOrCreateBuilder(parser, state, "counters/object", modifier);
            }

            public BlittableMetadataModifier GetMetadataModifierForNewDocument(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false, bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                return _cmd.GetOrCreateMetadataModifier(firstEtagOfLegacyRevision, legacyRevisionsCount, legacyImport, readLegacyEtag, operateOnTypes);
            }

            public Task<Stream> GetTempStreamAsync()
            {
                throw new NotSupportedException("GetTempStream is never used in CounterActions. Shouldn't happen");
            }
        }


        private sealed class TimeSeriesActions : ITimeSeriesActions
        {
            private readonly DocumentDatabase _database;
            private TimeSeriesHandler.SmugglerTimeSeriesBatchCommand _cmd;
            private TimeSeriesHandler.SmugglerTimeSeriesBatchCommand _prevCommand;
            private Task _prevCommandTask = Task.CompletedTask;
            private Size _batchSize;
            private readonly Size _maxBatchSize;

            public TimeSeriesActions(DocumentDatabase database)
            {
                _database = database;
                _cmd = new TimeSeriesHandler.SmugglerTimeSeriesBatchCommand(database);

                _maxBatchSize = new Size(
                    PlatformDetails.Is32Bits || database.Configuration.Storage.ForceUsing32BitsPager
                        ? 1
                        : 16,
                    SizeUnit.Megabytes);

                _batchSize = new Size();
            }

            private void AddToBatch(TimeSeriesItem item)
            {
                if (_cmd.AddToDictionary(item))
                {
                    // RavenDB-19504 - if we have a lot of _small_ updates, that can add up quickly, but it won't 
                    // be accounted for that if we look at segment size alone. So we assume that any new item means
                    // updating the whole segment. This is especially important for encrypted databases, where we need
                    // to keep all the modified data in memory in one shot
                    _batchSize.Add(2, SizeUnit.Kilobytes);
                }
                _batchSize.Add(item.Segment.NumberOfBytes, SizeUnit.Bytes);
            }

            private void AddToBatch(TimeSeriesDeletedRangeItemForSmuggler item)
            {
                _cmd.AddToDeletedRanges(item);

                var size = item.Name.Size +
                           item.DocId.Size +
                           item.Collection.Size +
                           item.ChangeVector.Size
                           + 3 * sizeof(long); // From, To, Etag

                _batchSize.Add(size, SizeUnit.Bytes);
            }

            public async ValueTask DisposeAsync()
            {
                await FinishBatchOfTimeSeriesAsync();
            }

            public async ValueTask WriteTimeSeriesAsync(TimeSeriesItem ts)
            {
                AddToBatch(ts);
                await HandleBatchOfTimeSeriesIfNecessaryAsync();
            }

            public async ValueTask WriteTimeSeriesDeletedRangeAsync(TimeSeriesDeletedRangeItemForSmuggler deletedRange)
            {
                AddToBatch(deletedRange);
                await HandleBatchOfTimeSeriesIfNecessaryAsync();
            }

            public void RegisterForDisposal(IDisposable data)
            {
                _cmd.AddToDisposal(data);
            }

            public void RegisterForReturnToTheContext(AllocatedMemoryData data)
            {
                _cmd.AddToReturn(data);
            }

            private async ValueTask HandleBatchOfTimeSeriesIfNecessaryAsync()
            {
                if (_batchSize < _maxBatchSize)
                    return;

                var prevCommand = _prevCommand;
                var prevCommandTask = _prevCommandTask;

                var commandTask = _database.TxMerger.Enqueue(_cmd);

                _prevCommand = _cmd;
                _prevCommandTask = commandTask;

                if (prevCommand != null)
                {
                    using (prevCommand)
                    {
                        await prevCommandTask;
                    }
                }

                _cmd = new TimeSeriesHandler.SmugglerTimeSeriesBatchCommand(_database);

                _batchSize.Set(0, SizeUnit.Bytes);
            }

            private async ValueTask FinishBatchOfTimeSeriesAsync()
            {
                if (_prevCommand != null)
                {
                    using (_prevCommand)
                    {
                        await _prevCommandTask;
                    }

                    _prevCommand = null;
                }

                if (_batchSize.GetValue(SizeUnit.Bytes) > 0)
                {
                    await _database.TxMerger.Enqueue(_cmd);
                }

                _cmd = null;
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _cmd.Context.CachedProperties.NewDocument();
                return _cmd.Context;
            }

            public BlittableJsonDocumentBuilder GetBuilderForNewDocument(UnmanagedJsonParser parser, JsonParserState state, BlittableMetadataModifier modifier = null)
            {
                return _cmd.GetOrCreateBuilder(parser, state, "timeseries/object", modifier);
            }

            public BlittableMetadataModifier GetMetadataModifierForNewDocument(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                return _cmd.GetOrCreateMetadataModifier(firstEtagOfLegacyRevision, legacyRevisionsCount, legacyImport, readLegacyEtag, operateOnTypes);
            }
        }
    }
}
