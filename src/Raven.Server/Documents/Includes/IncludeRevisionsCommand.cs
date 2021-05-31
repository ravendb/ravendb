using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public class IncludeRevisionsCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly HashSet<string> _revisions;

        public Dictionary<string, Document> Results { get;  }

        private IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;
            
            Results = new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, HashSet<string> revisions)
            : this(database, context)
        {
            _revisions = revisions;
        }

        public void Fill(Document document)
        {
            if (document == null)
                return;

            foreach (var fieldName in _revisions)
            {
                if (document.Data.TryGet(fieldName, out object singleOrMultipleCv) == false)
                {
                    throw new InvalidOperationException($"Cannot include revisions for related document '{document.Id}', " +
                                                        $"document {document.Id} doesn't have a field named '{fieldName}'. ");
                }
                if(singleOrMultipleCv is BlittableJsonReaderArray arr)
                {
                    
                    foreach (object cv in arr)
                    {
                        var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:cv.ToString());
                        Results.Add(cv.ToString(),getRevisionsByCv);
                    }
                }

            }
        }
    }
}
