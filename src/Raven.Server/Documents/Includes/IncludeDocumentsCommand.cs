using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Includes
{
    public class IncludeDocumentsCommand
    {
        private readonly DocumentsStorage _storage;
        private readonly DocumentsOperationContext _context;
        private readonly string[] _includes;
        private readonly bool _isProjection;

        private HashSet<string> _includedIds;

        private HashSet<string> _idsToIgnore;

        public IncludeDocumentsCommand(DocumentsStorage storage, DocumentsOperationContext context, string[] includes, bool isProjection)
        {
            _storage = storage;
            _context = context;
            _includes = includes;
            _isProjection = isProjection;
        }

        public void AddRange(HashSet<string> ids, string documentId)
        {
            AddToIgnore(documentId);

            if (ids == null)
                return;

            if (_includedIds == null)
                _includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            _includedIds.UnionWith(ids);
        }

        public void Gather(Document document)
        {
            if (document == null)
                return;

            if (_includes == null || _includes.Length == 0)
                return;

            if (_includedIds == null)
                _includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            AddToIgnore(document.Id);

            foreach (var include in _includes)
            {
                if (include == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                {
                    _includedIds.Add(document.Id);
                    continue;
                }
                IncludeUtil.GetDocIdFromInclude(document.Data, new StringSegment(include), _includedIds);
            }
        }

        public void Fill(List<Document> result)
        {
            if (_includedIds == null || _includedIds.Count == 0)
                return;

            foreach (var includedDocId in _includedIds)
            {
                if (string.IsNullOrEmpty(includedDocId))
                    continue;

                if (_idsToIgnore != null && _idsToIgnore.Contains(includedDocId))
                    continue;

                Document includedDoc;
                try
                {
                    includedDoc = _storage.Get(_context, includedDocId);
                    if (includedDoc == null)
                        continue;
                }
                catch (DocumentConflictException e)
                {
                    includedDoc = CreateConflictDocument(e);
                }

                result.Add(includedDoc);
            }
        }

        private static Document CreateConflictDocument(DocumentConflictException exception)
        {
            return new ConflictDocument(exception.DocId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToIgnore(string documentId)
        {
            if (_isProjection)
                return;

            if (documentId == null)
                return;

            if (_idsToIgnore == null)
                _idsToIgnore = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            _idsToIgnore.Add(documentId);
        }

        public class ConflictDocument : Document
        {
            public new readonly string Id;

            public ConflictDocument(string docId)
            {
                Id = docId;
            }
        }
    }
}
