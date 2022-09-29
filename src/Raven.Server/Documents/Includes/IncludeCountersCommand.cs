using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Handlers.Processors.Counters;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public class IncludeCountersCommand : ICounterIncludes
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, string[]> _countersBySourcePath;

        public Dictionary<string, string[]> IncludedCounterNames { get; }
        public Dictionary<string, List<CounterDetail>> Results { get; }

        private IncludeCountersCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;

            IncludedCounterNames = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
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
            if (document == null)
                return;

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
                IncludedCounterNames[docId] = countersToGet;

                var details = CountersHandlerProcessorForGetCounters.GetInternal(_database, _context, countersToGet, docId, false);
                Results.Add(docId, details.Counters);
            }
        }

        public async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token)
        {
            writer.WriteStartObject();

            var first = true;
            foreach (var kvp in Results)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(kvp.Key);

                await writer.WriteCountersForDocumentAsync(kvp.Value, token);
            }

            writer.WriteEndObject();
        }

        public int Count => Results?.Count ?? 0;
    }
}
