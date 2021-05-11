using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Includes
{
    public class IncludeRevisionsCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, string[]> _revisionsBySourcePath;

        public Dictionary<string, Document> Results { get;  }

        private IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;

            Results = new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, Dictionary<string, HashSet<string>> revisionsBySourcePath)
            : this(database, context)
        {
            _revisionsBySourcePath = revisionsBySourcePath.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray());
        }

        public void Fill(Document document)
        {
            if (document == null)
                return;

            var revisionChangeVectorField = string.Empty;
            
            foreach (var kvp in _revisionsBySourcePath)
            {
                if (kvp.Key != string.Empty && document.Data.TryGet(kvp.Key, out revisionChangeVectorField) == false)
                {
                    throw new InvalidOperationException($"Cannot include revisions for related document '{kvp.Key}', " + 
                                                        $"document {document.Id} doesn't have a field named '{kvp.Key}'. ");
                }

                // if (Results.ContainsKey(docId))
                //     continue;
                var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector: revisionChangeVectorField);
                
                Results.Add(revisionChangeVectorField,getRevisionsByCv);

            }
        }
    }
}
