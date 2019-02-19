using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Includes
{
    public class IncludeCountersCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, string[]> _countersBySourcePath;

        public Dictionary<string, string[]> CountersToGetByDocId { get; }
        public Dictionary<string, List<CounterDetail>> Results { get; }

        public IncludeCountersCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;

            CountersToGetByDocId = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
            Results = new Dictionary<string, List<CounterDetail>>(StringComparer.OrdinalIgnoreCase);
        }

        public IncludeCountersCommand(DocumentDatabase database, DocumentsOperationContext context, string[] counters) 
            : this(database, context)
        {
            _countersBySourcePath = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
            {
                [string.Empty] = counters
            };
        }

        public IncludeCountersCommand(DocumentDatabase database, DocumentsOperationContext context, Dictionary<string, HashSet<string>> countersBySourcePath)
            : this(database, context)
        {
            _countersBySourcePath = countersBySourcePath.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray());
        }

        public void Fill(Document document)
        {
            var docId = document.Id;

            foreach (var kvp in _countersBySourcePath)
            {
                if (kvp.Key != string.Empty &&
                    document.Data.TryGet(kvp.Key, out docId) == false)
                {
                    throw new InvalidOperationException($"Cannot include counters for related document '{kvp.Key}', " +
                                                        $"document {document.Id} doesn't have a field named '{kvp.Key}'. ");
                }

                if (Results.ContainsKey(docId))
                    continue;

                var countersToGet = kvp.Value.ToArray();
                CountersToGetByDocId[docId] = countersToGet;

                var details = CountersHandler.GetInternal(_database, _context, countersToGet, docId, false);
                Results.Add(docId, details.Counters);
            }
        }
    }
}
