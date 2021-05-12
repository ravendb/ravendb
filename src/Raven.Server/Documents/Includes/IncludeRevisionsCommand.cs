using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Linq;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Includes
{
    public class IncludeRevisionsCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly HashSet<string> _revisionsBySourcePath;

        public Dictionary<string, Document> Results { get;  }

        private IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;
            
            Results = new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, HashSet<string> revisionsBySourcePath)
            : this(database, context)
        {
            _revisionsBySourcePath = revisionsBySourcePath;
        }

        public void Fill(Document document)
        {
            if (document == null)
                return;
            
            foreach (var cv in _revisionsBySourcePath)
            {
                if (string.IsNullOrEmpty(cv))
                {
                    throw new InvalidOperationException($"Cannot include revisions for related document '{document.Id}', " + 
                                                        $"document {document.Id} doesn't have a field named '{cv}'. ");
                }

                var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector: cv);
                
                Results.Add(cv,getRevisionsByCv);

            }
        }
    }
}
