using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class GenAiConversationHandler(ServerStore server, DocumentDatabase database, bool enableTracing) : ConversationHandler(server, database)
{
    protected override Task<string> TryPersistAsync(JsonOperationContext context, List<BlittableJsonReaderObject> historyDocs)
    {
        if (enableTracing) 
            return base.TryPersistAsync(context, null);

        // don't persist the conversation document
        return Task.FromResult(Document.Id);
    }
}
