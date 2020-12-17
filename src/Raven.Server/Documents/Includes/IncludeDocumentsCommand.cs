using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

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

        public DocumentsOperationContext Context => _context;

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
                IncludeUtil.GetDocIdFromInclude(document.Data, new StringSegment(include), _includedIds, _storage.DocumentDatabase.IdentityPartsSeparator);
            }
        }

        public void Gather(List<FacetResult> results)
        {
            if (results == null || results.Count == 0)
                return;

            if (_includes == null || _includes.Length == 0)
                return;

            if (_includedIds == null)
                _includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var include in _includes)
            {
                string path = null;
                var match = results.FirstOrDefault(x => x.Name == include);
                if (match == null)
                {
                    int firstDot = include.IndexOf('.');
                    if (firstDot == -1)
                        continue;
                    string name = include.Substring(0, firstDot);
                    match = results.FirstOrDefault(x => x.Name == name);
                    if (match == null)
                        continue;
                    path = include.Substring(firstDot + 1);
                }
                foreach (FacetValue value in match.Values)
                {
                    if (path == null)
                    {
                        _includedIds.Add(value.Range);
                    }
                    else
                    {
                        BlittableJsonReaderObject json;
                        try
                        {
                            json = _context.ReadForMemory(value.Range, "Facet/Object");
                        }
                        catch (Exception)
                        {
                            // expected, we can ignore this
                            continue;
                        }

                        using (json)
                        {
                            IncludeUtil.GetDocIdFromInclude(json, new StringSegment(path), _includedIds, _context.DocumentDatabase.IdentityPartsSeparator);
                        }
                    }
                }
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

            _idsToIgnore ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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
