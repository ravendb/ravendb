using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Global;
using Sparrow;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseDestination : ISmugglerDestination
    {
        private readonly DocumentDatabase _database;

        private readonly Logger _log;
        private BuildVersionType _buildType;

        public DatabaseDestination(DocumentDatabase database)
        {
            _database = database;
            _log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion)
        {
            _buildType = BuildVersion.Type(buildVersion);
            return null;
        }

        public IDocumentActions Documents()
        {
            return new DatabaseDocumentActions(_database, _buildType, isRevision: false, log: _log);
        }

        public IDocumentActions RevisionDocuments()
        {
            return new DatabaseDocumentActions(_database, _buildType, isRevision: true, log: _log);
        }
     
        public IIdentityActions Identities()
        {
            return new DatabaseIdentityActions(_database);
        }

        public IIndexActions Indexes()
        {
            return new DatabaseIndexActions(_database);
        }

        private class DatabaseIndexActions : IIndexActions
        {
            private readonly DocumentDatabase _database;

            public DatabaseIndexActions(DocumentDatabase database)
            {
                _database = database;
            }

            public void WriteIndex(IndexDefinitionBase indexDefinition, IndexType indexType)
            {
                AsyncHelpers.RunSync(() => _database.IndexStore.CreateIndex(indexDefinition));
            }

            public void WriteIndex(IndexDefinition indexDefinition)
            {
                AsyncHelpers.RunSync(() => _database.IndexStore.CreateIndex(indexDefinition));
            }

            public void Dispose()
            {
            }
        }

        public class DatabaseDocumentActions : IDocumentActions
        {
            private readonly DocumentDatabase _database;
            private readonly BuildVersionType _buildType;
            private readonly bool _isRevision;
            private readonly Logger _log;
            private MergedBatchPutCommand _command;
            private MergedBatchPutCommand _prevCommand;
            private Task _prevCommandTask = Task.CompletedTask;

            private readonly Sparrow.Size _enqueueThreshold;

            public DatabaseDocumentActions(DocumentDatabase database, BuildVersionType buildType, bool isRevision, Logger log)
            {
                _database = database;
                _buildType = buildType;
                _isRevision = isRevision;
                _log = log;
                _enqueueThreshold = new Sparrow.Size(
                    (sizeof(int) == IntPtr.Size || database.Configuration.Storage.ForceUsing32BitsPager) ? 2 : 32,
                    SizeUnit.Megabytes);

                _command = new MergedBatchPutCommand(database, buildType, log)
                {
                    IsRevision = isRevision
                };
            }

            public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (item.Attachments != null)
                    progress.Attachments.ReadCount += item.Attachments.Count;
                _command.Add(item);
                HandleBatchOfDocumentsIfNecessary();
            }

            public Stream GetTempStream()
            {
                if(_command.AttachmentStreamsTempFile == null)
                    _command.AttachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("smuggler");

                return _command.AttachmentStreamsTempFile.StartNewStream();
            }

            public DocumentsOperationContext GetContextForNewDocument()
            {
                _command.Context.CachedProperties.NewDocument();
                return _command.Context;
            }

            public void Dispose()
            {
                FinishBatchOfDocuments();
            }

            private void HandleBatchOfDocumentsIfNecessary()
            {
                var prevDoneAndHasEnough = _command.Context.AllocatedMemory > Constants.Size.Megabyte && _prevCommandTask.IsCompleted;
                var currentReachedLimit = _command.Context.AllocatedMemory > _enqueueThreshold.GetValue(SizeUnit.Bytes);

                if (currentReachedLimit == false && prevDoneAndHasEnough == false)
                    return;

                var prevCommand = _prevCommand;
                var prevCommandTask = _prevCommandTask;

                _prevCommand = _command;
                _prevCommandTask = _database.TxMerger.Enqueue(_command).AsTask();

                if (prevCommand != null)
                {
                    using (prevCommand)
                    {
                        prevCommandTask.Wait();
                        Debug.Assert(prevCommand.IsDisposed == false,
                            "we rely on reusing this context on the next batch, so it has to be disposed here");
                    }
                }

                _command = new MergedBatchPutCommand(_database, _buildType, _log)
                {
                    IsRevision = _isRevision
                };
            }

            private void FinishBatchOfDocuments()
            {
                if (_prevCommand != null)
                {
                    using (_prevCommand)
                        AsyncHelpers.RunSync(() => _prevCommandTask);

                    _prevCommand = null;
                }

                if (_command.Documents.Count > 0)
                {
                    using (_command)
                        AsyncHelpers.RunSync(() => _database.TxMerger.Enqueue(_command).AsTask());
                }

                _command = null;
            }
        }

        private class DatabaseIdentityActions : IIdentityActions
        {
            private readonly DocumentDatabase _database;
            private readonly Dictionary<string, long> _identities;

            public DatabaseIdentityActions(DocumentDatabase database)
            {
                _database = database;
                _identities = new Dictionary<string, long>();
            }

            public void WriteIdentity(string key, long value)
            {
                _identities[key] = value;
            }

            public void Dispose()
            {
                if (_identities.Count == 0)
                    return;

                //fire and forget, do not hold-up smuggler operations waiting for Raft command
                _database.ServerStore.SendToLeaderAsync(new UpdateClusterIdentityCommand(_database.Name, _identities)).Wait();
            }
        }

        private class MergedBatchPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            public bool IsRevision;

            private readonly DocumentDatabase _database;
            private readonly BuildVersionType _buildType;
            private readonly Logger _log;

            public readonly List<DocumentItem> Documents = new List<DocumentItem>();
            public StreamsTempFile AttachmentStreamsTempFile;

            private IDisposable _resetContext;
            private bool _isDisposed;

            public bool IsDisposed => _isDisposed;

            private readonly DocumentsOperationContext _context;
            private const string PreV4RevisionsDocumentId = "/revisions/";

            public MergedBatchPutCommand(DocumentDatabase database, BuildVersionType buildType, Logger log)
            {
                _database = database;
                _buildType = buildType;
                _log = log;
                _resetContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            public DocumentsOperationContext Context => _context;

            public override int Execute(DocumentsOperationContext context)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Importing {Documents.Count:#,#} documents");

                foreach (var documentType in Documents)
                {
                    if (documentType.Attachments != null)
                    {
                        foreach (var attachment in documentType.Attachments)
                        {
                            using (attachment)
                            {
                                _database.DocumentsStorage.AttachmentsStorage.PutAttachmentStream(context, attachment.Tag, attachment.Base64Hash, attachment.Stream);
                            }
                        }
                    }

                    var document = documentType.Document;
                    using (document.Data)
                    {
                        var id = document.Id;

                        if (IsRevision)
                        {
                            if (_database.DocumentsStorage.RevisionsStorage.Configuration == null)
                                ThrowRevisionsDisabled();

                            PutAttachments(context, document);
                            _database.DocumentsStorage.RevisionsStorage.Put(context, id, document.Data, document.Flags, 
                                document.NonPersistentFlags, document.ChangeVector, document.LastModified.Ticks);
                            continue;
                        }

                        if (IsPreV4Revision(id, document))
                        {
                            // handle old revisions
                            if (_database.DocumentsStorage.RevisionsStorage.Configuration == null)
                                ThrowRevisionsDisabled();

                            var endIndex = id.IndexOf(PreV4RevisionsDocumentId, StringComparison.OrdinalIgnoreCase);
                            var newId = id.Substring(0, endIndex);

                            _database.DocumentsStorage.RevisionsStorage.Put(context, newId, document.Data, document.Flags, document.NonPersistentFlags, document.ChangeVector, document.LastModified.Ticks);
                            continue;
                        }

                        PutAttachments(context, document);
                        _database.DocumentsStorage.Put(context, id, null, document.Data, null, null, document.Flags, document.NonPersistentFlags);
                    }
                }

                return Documents.Count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private unsafe void PutAttachments(DocumentsOperationContext context, Document document)
            {
                if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                    return;

                if (document.Data.TryGet(Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false || 
                    metadata.TryGet(Client.Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false || 
                        attachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue contentType) == false || 
                        attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                        throw new ArgumentException($"The attachment info in missing a mandatory value: {attachment}");

                    var type = (document.Flags & DocumentFlags.Revision) == DocumentFlags.Revision ? AttachmentType.Revision : AttachmentType.Document;
                    var attachmentsStorage = _database.DocumentsStorage.AttachmentsStorage;
                    using (DocumentIdWorker.GetSliceFromId(_context, document.Id, out Slice lowerDocumentId))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(_context, name, out Slice lowerName, out Slice nameSlice))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(_context, contentType, out Slice lowerContentType, out Slice contentTypeSlice))
                    using (Slice.External(_context.Allocator, hash, out Slice base64Hash))
                    using(Slice.From(_context.Allocator, document.ChangeVector, out var cv))
                    using (attachmentsStorage.GetAttachmentKey(_context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size,
                        base64Hash, lowerContentType.Content.Ptr, lowerContentType.Size, type, cv, out Slice keySlice))
                    {
                        attachmentsStorage.PutDirect(context, keySlice, nameSlice, contentTypeSlice, base64Hash, null);
                    }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool IsPreV4Revision(string id, Document document)
            {
                if (_buildType == BuildVersionType.V3 == false)
                    return false;

                if ((document.NonPersistentFlags & NonPersistentDocumentFlags.LegacyRevision) != NonPersistentDocumentFlags.LegacyRevision)
                    return false;

                return id.Contains(PreV4RevisionsDocumentId);
            }

            private static void ThrowRevisionsDisabled()
            {
                throw new InvalidOperationException("Revisions needs to be enabled before import!");
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;

                Documents.Clear();
                _resetContext?.Dispose();
                _resetContext = null;

                AttachmentStreamsTempFile?.Dispose();
                AttachmentStreamsTempFile = null;
            }

            public void Add(DocumentItem document)
            {
                Documents.Add(document);
            }
        }
    }
}
