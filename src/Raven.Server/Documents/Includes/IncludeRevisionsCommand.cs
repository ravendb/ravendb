using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public class IncludeRevisionsCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly HashSet<string> _pathsForRevisionsInDocuments;

        public Dictionary<string, Document> Results { get; private set; }

        private IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context  = context;
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, HashSet<string> pathsForRevisionsInDocuments)
            : this(database, context)
        {
            _pathsForRevisionsInDocuments = pathsForRevisionsInDocuments ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public void Fill(Document document)
        {
            if (document == null)
                return;

            foreach (var fieldName in _pathsForRevisionsInDocuments)
            {
                if (document.Data.TryGet(fieldName, out object singleOrMultipleCv) == false  )
                    return;
                
                switch (singleOrMultipleCv)
                {
                    case BlittableJsonReaderArray blittableJsonReaderArray:
                    {
                        foreach (object cvObj in blittableJsonReaderArray)
                        {
                            var changeVector = Convert.ToString(cvObj);
                            var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:changeVector);
                            Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                            Results[changeVector] = getRevisionsByCv;
                        }
                        break;
                    }
                    
                    case LazyStringValue lazyStringValue:
                    {
                        var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:lazyStringValue);
                        Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                        Results[lazyStringValue] = getRevisionsByCv;
                        break;
                    }
                    
                    case LazyCompressedStringValue lazyCompressedStringValue:
                    {
                        var toLazyStringValue = lazyCompressedStringValue.ToLazyStringValue();
                        var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:toLazyStringValue);
                        Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                        Results[toLazyStringValue] = getRevisionsByCv;
                        break;
                    }
                }
            }
        }

        public void AddRange(HashSet<string> revisionsCvs)
        {
            if (revisionsCvs is null)
                return;
            
            foreach (string revisionsCv in revisionsCvs)
            {
                var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:revisionsCv);
                Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                Results[revisionsCv] = getRevisionsByCv;
            }  
        }
    }
}
