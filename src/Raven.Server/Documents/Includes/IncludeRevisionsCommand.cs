using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public class IncludeRevisionsCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly DateTime? _revisionsBeforeDateTime;
        private readonly HashSet<string> _pathsForRevisionsChangeVectors;
        private readonly HashSet<string> _revisionsChangeVectors;
        public Dictionary<string, Document> RevisionsChangeVectorResults { get; private set; }
        public Dictionary<string, Dictionary<DateTime, Document>> IdByRevisionsByDateTimeResults { get; private set; }

        private IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context  = context;
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, RevisionIncludeField revisionIncludeField): this(database, context)
        {
            _revisionsChangeVectors = revisionIncludeField?.RevisionsChangeVectors ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _pathsForRevisionsChangeVectors = revisionIncludeField?.RevisionsChangeVectorsPaths ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase); 
            _revisionsBeforeDateTime = revisionIncludeField?.RevisionsBeforeDateTime ?? new DateTime();
        }

        public void Fill(Document document)
        {
            if (document == null)
                return;
            
            if (_revisionsBeforeDateTime != default(DateTime))
            {
                var doc = _database.DocumentsStorage.RevisionsStorage.GetRevisionBefore(context: _context, id: document.Id, max: _revisionsBeforeDateTime.Value); 
                if (doc is null) return; 
                RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase); 
                IdByRevisionsByDateTimeResults ??= new Dictionary<string, Dictionary<DateTime, Document>>(StringComparer.OrdinalIgnoreCase); 
                RevisionsChangeVectorResults[doc.ChangeVector] = doc;
                IdByRevisionsByDateTimeResults[document.Id] = new Dictionary<DateTime, Document> (){{_revisionsBeforeDateTime.Value, doc}};
            }

            if (_revisionsChangeVectors?.Count > 0)
            {
                foreach (var changeVector in _revisionsChangeVectors)
                {
                    RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                    if (RevisionsChangeVectorResults.ContainsKey(changeVector))
                        continue;
                    var doc  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:changeVector);
                    if (doc is null) return;
                    RevisionsChangeVectorResults[changeVector] = doc;
                }
            }

            if (_pathsForRevisionsChangeVectors?.Count > 0)
            {
                  foreach (var path in _pathsForRevisionsChangeVectors)
                  {
                      var bt = BlittableJsonTraverser.Default;
                      if (bt.TryRead(document.Data, path, out var singleOrMultipleCv, out var _) == false)
                        throw new InvalidOperationException($"Field `{path}` (which is mentioned inside `include revisions(..)`) is missing in document.");

                      switch (singleOrMultipleCv)
                      {
                          case BlittableJsonReaderArray blittableJsonReaderArray:
                          {
                              foreach (object cvObj in blittableJsonReaderArray)
                              {
                                  var changeVector = Convert.ToString(cvObj);
                                  RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                                  if (RevisionsChangeVectorResults.ContainsKey(changeVector))
                                      continue;
                                  var doc  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:changeVector);
                                  if (doc is null) return;
                                  RevisionsChangeVectorResults[changeVector] = doc;
                              }
                              break;
                          }
                                    
                          case LazyStringValue cvAsLazyStringValue:
                          {
                              RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                              if (RevisionsChangeVectorResults.ContainsKey(cvAsLazyStringValue))
                                  continue;
                              var doc  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:cvAsLazyStringValue);
                              if (doc is null) return;
                              RevisionsChangeVectorResults[cvAsLazyStringValue] = doc;
                              break;
                          }
                                    
                          case LazyCompressedStringValue cvAsLazyCompressedStringValue:
                          {
                              var cvAsLazyStringValue = cvAsLazyCompressedStringValue.ToLazyStringValue();
                              RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                              if (RevisionsChangeVectorResults.ContainsKey(cvAsLazyStringValue))
                                  continue;
                              var doc  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:cvAsLazyStringValue);
                              if (doc is null) return;
                              RevisionsChangeVectorResults[cvAsLazyStringValue] = doc;
                              break;
                          }
                      }
                  }
            }
          
        }
        public void AddRange(HashSet<string> changeVectorPaths)
        {
            if (changeVectorPaths is null)
                return;
            
            foreach (string changeVector in changeVectorPaths)
            {
                var doc  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:changeVector);
                if (doc is null) return;
                RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                RevisionsChangeVectorResults[changeVector] = doc;
            }  
        }
        
        public void AddRevisionByDateTimeBefore(DateTime? dateTime, string documentId)
        {
            if (dateTime.HasValue == false)
                return;

            var doc = _database.DocumentsStorage.RevisionsStorage.GetRevisionBefore(context: _context, id: documentId, max: dateTime.Value); 
            if (doc is null)
                return;
            
            RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase); 
            IdByRevisionsByDateTimeResults ??= new Dictionary<string, Dictionary<DateTime, Document>>(StringComparer.OrdinalIgnoreCase); 
            RevisionsChangeVectorResults[doc.ChangeVector] = doc;
            IdByRevisionsByDateTimeResults[documentId] = new Dictionary<DateTime, Document> (){{dateTime.Value, doc}};
        }

       
    }
}
