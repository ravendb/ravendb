using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Includes
{
    public unsafe class IncludeDocumentsDuringTimeSeriesLoadingCommand
    {
        private readonly DocumentsOperationContext _context;
        private readonly string _docId;
        private readonly bool _includeDoc;
        private readonly bool _includeTags;

        private byte* _state;
        private readonly Dictionary<string, BlittableJsonReaderObject> _includesDictionary;
        private DynamicJsonValue _includes;
        private  readonly HashSet<string> _missingIncludes;

        public IncludeDocumentsDuringTimeSeriesLoadingCommand(DocumentsOperationContext context, string docId, bool includeDocument, bool includeTags)
        {
            _docId = docId ?? throw new ArgumentException(nameof(docId));
            _context = context;
            _includeDoc = includeDocument;
            _includeTags = includeTags;

            _includesDictionary = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            _missingIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public void InitializeNewRangeResult(byte* state)
        {
            _state = state;
            _includes = new DynamicJsonValue();
            _missingIncludes.Clear();

            if (_includeDoc == false)
                return;

            IncludeDocument(_docId);
        }

        public void Fill(string tag)
        {
            if (_includeTags == false || tag == null)
                return;

            IncludeDocument(tag);
        }

        public void AddIncludesToResult(TimeSeriesRangeResult rangeResult)
        {
            if (rangeResult == null)
                return;

            rangeResult.MissingIncludes = _missingIncludes.ToList();

            if (_includes?.Properties.Count > 0 == false)
                return;
            
            rangeResult.Includes = _context.ReadObject(_includes, "TimeSeriesRangeIncludes/" + _docId);
        }
        
        private void IncludeDocument(string id)
        {
            if (_includesDictionary.ContainsKey(id))
            {
                if (_includesDictionary[id] == null && _missingIncludes.Contains(id) == false)
                    _missingIncludes.Add(id);
                return;
            }
            
            var doc = _context.DocumentDatabase.DocumentsStorage.Get(_context, id, throwOnConflict: false);
            doc?.EnsureMetadata();
            _includesDictionary[id] = doc?.Data;
            
            ComputeHttpEtags.HashChangeVector(_state, doc?.ChangeVector);

            if (doc?.Data == null)
            {
                _missingIncludes.Add(id);
                return;
            }

            _includes[id] = doc.Data;
        }
    }
}
