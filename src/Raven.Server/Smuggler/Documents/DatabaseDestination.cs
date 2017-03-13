using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Logging;
using Size = Raven.Server.Config.Settings.Size;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseDestination : ISmugglerDestination
    {
        private readonly DocumentDatabase _database;
        private long _buildVersion;

        private Logger _log;

        public DatabaseDestination(DocumentDatabase database)
        {
            _database = database;
            _log = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name);
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion)
        {
            _buildVersion = buildVersion;
            return null;
        }

        public IDocumentActions Documents()
        {
            return new DatabaseDocumentActions(_database, _buildVersion, isRevision: false, log: _log);
        }

        public IDocumentActions RevisionDocuments()
        {
            return new DatabaseDocumentActions(_database, _buildVersion, isRevision: true, log: _log);
        }

        public IIdentityActions Identities()
        {
            return new DatabaseIdentityActions(_database);
        }

        public IIndexActions Indexes()
        {
            return new DatabaseIndexActions(_database);
        }

        public ITransformerActions Transformers()
        {
            return new DatabaseTransformerActions(_database);
        }

        private class DatabaseTransformerActions : ITransformerActions
        {
            private readonly DocumentDatabase _database;

            public DatabaseTransformerActions(DocumentDatabase database)
            {
                _database = database;
            }

            public void WriteTransformer(TransformerDefinition transformerDefinition)
            {
                _database.TransformerStore.CreateTransformer(transformerDefinition);
            }

            public void Dispose()
            {
            }
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
                _database.IndexStore.CreateIndex(indexDefinition);
            }

            public void WriteIndex(IndexDefinition indexDefinition)
            {
                _database.IndexStore.CreateIndex(indexDefinition);
            }

            public void Dispose()
            {
            }
        }

        private class DatabaseDocumentActions : IDocumentActions
        {
            private readonly DocumentDatabase _database;
            private readonly long _buildVersion;
            private readonly bool _isRevision;
            private readonly Logger _log;
            private MergedBatchPutCommand _command;
            private MergedBatchPutCommand _prevCommand;
            private Task _prevCommandTask;

            private readonly Size _enqueueThreshold;

            public DatabaseDocumentActions(DocumentDatabase database, long buildVersion, bool isRevision, Logger log)
            {
                _database = database;
                _buildVersion = buildVersion;
                _isRevision = isRevision;
                _log = log;
                _enqueueThreshold = new Size(
                    (sizeof(int) == IntPtr.Size || database.Configuration.Storage.ForceUsing32BitsPager) ? 2 : 32,
                    SizeUnit.Megabytes);

                _command = new MergedBatchPutCommand(database, buildVersion, log)
                {
                    IsRevision = isRevision
                };
            }

            public void WriteDocument(Document document)
            {
                _command.Add(document);

                HandleBatchOfDocumentsIfNecessary();
            }

            public JsonOperationContext GetContextForNewDocument()
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
                if (_command.Context.AllocatedMemory < _enqueueThreshold.GetValue(SizeUnit.Bytes))
                    return;

                if (_prevCommand != null)
                {
                    using (_prevCommand)
                    {
                        _prevCommandTask.Wait();
                        Debug.Assert(_prevCommand.IsDisposed == false,
                            "we rely on reusing this context on the next batch, so it has to be disposed here");
                    }
                }

                _prevCommand = _command;
                _prevCommandTask = _database.TxMerger.Enqueue(_command);

                _command = new MergedBatchPutCommand(_database, _buildVersion, _log)
                {
                    IsRevision = _isRevision,
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
                        AsyncHelpers.RunSync(() => _database.TxMerger.Enqueue(_command));
                }

                _command = null;
            }
        }

        private class DatabaseIdentityActions : IIdentityActions
        {
            private readonly DocumentDatabase _database;
            private readonly DocumentsOperationContext _context;
            private readonly Dictionary<string, long> _identities;
            private readonly IDisposable _returnContext;

            public DatabaseIdentityActions(DocumentDatabase database)
            {
                _database = database;
                _returnContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
                _identities = new Dictionary<string, long>();
            }

            public void WriteIdentity(string key, long value)
            {
                _identities[key] = value;
            }

            public void Dispose()
            {
                try
                {
                    if (_identities.Count == 0)
                        return;

                    using (var tx = _context.OpenWriteTransaction())
                    {
                        _database.DocumentsStorage.UpdateIdentities(_context, _identities);

                        tx.Commit();
                    }
                }
                finally
                {
                    _returnContext?.Dispose();
                }
            }
        }

        private class MergedBatchPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            public bool IsRevision;

            private readonly DocumentDatabase _database;
            private readonly long _buildVersion;
            private readonly Logger _log;

            public Size TotalSize = new Size(0, SizeUnit.Bytes);

            public readonly List<Document> Documents = new List<Document>();
            private IDisposable _resetContext;
            private bool _isDisposed;

            public bool IsDisposed => _isDisposed;

            private readonly DocumentsOperationContext _context;

            public MergedBatchPutCommand(DocumentDatabase database, long buildVersion, Logger log)
            {
                _database = database;
                _buildVersion = buildVersion;
                _log = log;
                _resetContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            public JsonOperationContext Context => _context;

            public override void Execute(DocumentsOperationContext context)
            {
                if(_log.IsInfoEnabled)
                    _log.Info($"Importing {Documents.Count:#,#} documents");

                foreach (var document in Documents)
                {
                    var key = document.Key;

                    if (IsRevision)
                    {
                        _database.BundleLoader.VersioningStorage.PutDirect(context, key, document.Data, document.ChangeVector);
                    }
                    else if (_buildVersion < 40000 && key.Contains("/revisions/"))
                    {
                        var endIndex = key.IndexOf("/revisions/", StringComparison.OrdinalIgnoreCase);
                        var newKey = key.Substring(0, endIndex);

                        _database.BundleLoader.VersioningStorage.PutDirect(context, newKey, document.Data, document.ChangeVector);
                    }
                    else
                    {
                        _database.DocumentsStorage.Put(context, key, null, document.Data);
                    }
                }
            }

            private static void ThrowDocumentMustHaveMetadata()
            {
                throw new InvalidOperationException("A document must have a metadata");
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;
                for (int i = Documents.Count - 1; i >= 0; i--)
                {
                    Documents[i].Data.Dispose();
                }

                Documents.Clear();
                _resetContext?.Dispose();
                _resetContext = null;
            }

            public void Add(Document document)
            {
                Documents.Add(document);
                TotalSize.Add(document.Data.Size, SizeUnit.Bytes);
            }
        }
    }
}