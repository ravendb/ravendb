﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseSource : ISmugglerSource
    {
        private readonly DocumentDatabase _database;
        private DocumentsOperationContext _context;
        
        private readonly long _startDocumentEtag;

        private DatabaseSmugglerOptions _options;
        private SmugglerResult _result;

        private IDisposable _returnContext;
        private IDisposable _disposeTransaction;

        private int _currentTypeIndex;

        private readonly DatabaseItemType[] _types =
        {
            DatabaseItemType.Documents,
            DatabaseItemType.RevisionDocuments,
            DatabaseItemType.Indexes,
            DatabaseItemType.Transformers,
            DatabaseItemType.LocalIdentities,
            DatabaseItemType.ClusterIdentities,
            DatabaseItemType.None
        };

        public DatabaseSource(DocumentDatabase database, long startDocumentEtag)
        {
            _database = database;
            _startDocumentEtag = startDocumentEtag;
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion)
        {
            _currentTypeIndex = 0;
            _options = options;
            _result = result;
            _returnContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            _disposeTransaction = _context.OpenReadTransaction();

            buildVersion = ServerVersion.Build;
            return new DisposableAction(() =>
            {
                _disposeTransaction.Dispose();
                _returnContext.Dispose();
            });
        }

        public DatabaseItemType GetNextType()
        {
            return _types[_currentTypeIndex++];
        }

        public IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            var documents = collectionsToExport.Count != 0
                ? _database.DocumentsStorage.GetDocumentsFrom(_context, collectionsToExport, _startDocumentEtag, int.MaxValue)
                : _database.DocumentsStorage.GetDocumentsFrom(_context, _startDocumentEtag, 0, int.MaxValue);

            foreach (var document in documents)
            {
                yield return new DocumentItem
                {
                    Document = document,
                };
            }
        }

        public IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions)
        {
            var versioningStorage = _database.DocumentsStorage.VersioningStorage;
            if (versioningStorage.Configuration == null)
                yield break;

            var documents = versioningStorage.GetRevisionsFrom(_context, _startDocumentEtag, int.MaxValue);
            foreach (var document in documents)
            {
                yield return new DocumentItem
                {
                    Document = document,
                };
            }
        }

        public Stream GetAttachmentStream(LazyStringValue hash)
        {
            using (Slice.External(_context.Allocator, hash, out Slice hashSlice))
            {
                return _database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(_context, hashSlice);
            }
        }

        public IEnumerable<IndexDefinitionAndType> GetIndexes()
        {
            foreach (var index in _database.IndexStore.GetIndexes())
            {
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

        public IEnumerable<TransformerDefinition> GetTransformers()
        {
            foreach (var transformer in _database.TransformerStore.GetTransformers())
            {
                yield return transformer.Definition;
            }
        }

        public IEnumerable<KeyValuePair<string, long>> GetLocalIdentities()
        {
            return _database.DocumentsStorage.Identities.GetIdentities(_context);
        }

        public IEnumerable<KeyValuePair<string, long>> GetClusterIdentities()
        {
            var dr = _database.ServerStore.LoadDatabaseRecord(_database.Name, out long _);
            return dr.Identities;
        }


        public long SkipType(DatabaseItemType type)
        {
            return 0; // no-op
        }
    }
}