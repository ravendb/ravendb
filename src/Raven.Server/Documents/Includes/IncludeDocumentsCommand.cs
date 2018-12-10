using System;
using System.Collections.Generic;
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

        private HashSet<string> _includedIds;

        public IncludeDocumentsCommand(DocumentsStorage storage, DocumentsOperationContext context, string[] includes)
        {
            _storage = storage;
            _context = context;
            _includes = includes;
        }

        public void AddRange(HashSet<string> ids)
        {
            if (ids == null)
                return;

            if (_includedIds == null)
                _includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            _includedIds.UnionWith(ids);
        }

        public void Add(string id)
        {
            if (_includedIds == null)
                _includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            _includedIds.Add(id);
        }

        public void Gather(Document document)
        {
            if (document == null)
                return;

            if (_includes == null || _includes.Length == 0)
                return;

            if (_includedIds == null)
                _includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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
