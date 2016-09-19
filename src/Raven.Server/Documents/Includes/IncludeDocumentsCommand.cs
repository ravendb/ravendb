using System;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using StringSegment = Sparrow.StringSegment;

namespace Raven.Server.Documents.Includes
{
    public class IncludeDocumentsCommand
    {
        private readonly DocumentsStorage _storage;
        private readonly DocumentsOperationContext _context;
        private readonly StringValues _includes;

        private HashSet<string> _includedIds;

        public IncludeDocumentsCommand(DocumentsStorage storage, DocumentsOperationContext context, StringValues includes)
        {
            _storage = storage;
            _context = context;
            _includes = includes;
        }

        public void Add(string id)
        {
            _includedIds.Add(id);
        }

        public void Gather(Document document)
        {
            if (_includes.Count == 0)
                return;

            if (_includedIds == null)
                _includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var include in _includes)
                IncludeUtil.GetDocIdFromInclude(document.Data, new StringSegment(include, 0), _includedIds);
        }

        public void Fill(List<Document> result)
        {
            if (_includedIds == null || _includedIds.Count == 0)
                return;

            foreach (var includedDocId in _includedIds)
            {
                if (includedDocId == null) //precaution, should not happen
                    continue;

                var includedDoc = _storage.Get(_context, includedDocId);
                if (includedDoc == null)
                    continue;

                result.Add(includedDoc);
            }
        }
    }
}