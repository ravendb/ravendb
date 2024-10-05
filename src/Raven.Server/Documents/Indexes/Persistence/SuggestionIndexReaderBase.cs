using System.Threading;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class SuggestionIndexReaderBase : IndexOperationBase
    {
        protected SuggestionIndexReaderBase(Index index, RavenLogger logger) : base(index, logger)
        {
        }

        public abstract SuggestionResult Suggestions(IndexQueryServerSide query, SuggestionField field, JsonOperationContext documentsContext, CancellationToken token);
    }
}
