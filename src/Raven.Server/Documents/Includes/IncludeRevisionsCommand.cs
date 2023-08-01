using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public sealed class IncludeRevisionsCommand : IRevisionIncludes
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly DateTime? _revisionsBeforeDateTime;
        private readonly HashSet<string> _pathsForRevisionsChangeVectors;
        public Dictionary<string, Document> RevisionsChangeVectorResults { get; private set; }
        public Dictionary<string, Dictionary<DateTime, Document>> IdByRevisionsByDateTimeResults { get; private set; }

        private IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context  = context;
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, RevisionIncludeField revisionIncludeField): this(database, context)
        {
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


        public async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token)
        {
            var first = true;
            if (IdByRevisionsByDateTimeResults != null)
            {
                foreach ((string id, var dateTimeToDictionary) in IdByRevisionsByDateTimeResults)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    foreach ((DateTime dateTime, Document doc) in dateTimeToDictionary)
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(RevisionIncludeResult.Id));
                        writer.WriteString(id);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(RevisionIncludeResult.ChangeVector));
                        writer.WriteString(doc.ChangeVector);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(RevisionIncludeResult.Before));
                        writer.WriteDateTime(dateTime, true);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(RevisionIncludeResult.Revision));
                        writer.WriteDocument(context, metadataOnly: false, document: doc);
                        writer.WriteEndObject();

                        await writer.MaybeFlushAsync(token);
                    }
                }
            }
            if (RevisionsChangeVectorResults != null)
            {
                foreach ((string key, Document document) in RevisionsChangeVectorResults)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(RevisionIncludeResult.ChangeVector));
                    writer.WriteString(key);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(RevisionIncludeResult.Id));
                    writer.WriteString(document.Id);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(RevisionIncludeResult.Revision));
                    writer.WriteDocument(context, metadataOnly: false, document: document);
                    await writer.MaybeFlushAsync(token);

                    writer.WriteEndObject();
                }
            }
            await writer.MaybeFlushAsync(token);
        }

        public int Count => RevisionsChangeVectorResults?.Count ?? 0 + IdByRevisionsByDateTimeResults?.Count ?? 0;
    }
}
