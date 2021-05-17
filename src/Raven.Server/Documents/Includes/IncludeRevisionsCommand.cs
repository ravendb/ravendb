using System;
using System.Collections.Generic;
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
            
            var revisionCV = string.Empty;

            foreach (var fieldName in _revisionsBySourcePath)
            {
                if ( (fieldName != string.Empty && document.Data.TryGet(fieldName, out revisionCV)) == false)
                {
                    throw new InvalidOperationException($"Cannot include revisions for related document '{document.Id}', " + 
                                                        $"document {document.Id} doesn't have a field named '{fieldName}'. ");
                }

                var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector: revisionCV);
                 
                 Results.Add(revisionCV,getRevisionsByCv);

            }
        }
    }
}
