using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Includes
{
    public class IncludeCountersCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly StringValues _counters;

        public Dictionary<string, List<CounterDetail>> Results { get; }

        public IncludeCountersCommand(DocumentDatabase database, DocumentsOperationContext context, StringValues counters)
        {
            _database = database;
            _context = context;
            _counters = counters;
            Results = new Dictionary<string, List<CounterDetail>>();
        }

        public void Fill(string docId)
        {
            var details = CountersHandler.GetInternal(_database, _context, _counters, docId, false);
            Results.Add(docId, details.Counters);
        }

    }
}
