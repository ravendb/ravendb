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

        public IncludeDocumentsCommand(DocumentsStorage storage, DocumentsOperationContext context, StringValues includes)
        {
            _storage = storage;
            _context = context;
            _includes = includes;
        }

        public void Execute(List<Document> documents, List<Document> includedDocs)
        {
            var includedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var include in _includes)
            {
                foreach (var doc in documents)
                {
                    IncludeUtil.GetDocIdFromInclude(doc.Data, new StringSegment(include, 0), includedIds);
                }
            }

            foreach (var includedDocId in includedIds)
            {
                if (includedDocId == null) //precaution, should not happen
                    continue;

                var includedDoc = _storage.Get(_context, includedDocId);
                if (includedDoc == null)
                    continue;

                includedDocs.Add(includedDoc);
            }
        }
    }
}