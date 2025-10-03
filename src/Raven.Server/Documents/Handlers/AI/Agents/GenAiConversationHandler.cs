using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class GenAiConversationHandler(ServerStore server, DocumentDatabase database) : ConversationHandler(server, database)
{
    protected override Task<string> TryPersistAsync(JsonOperationContext context, List<BlittableJsonReaderObject> historyDocs)
    {
        // In GenAI mode, we don't persist the conversation document
        return Task.FromResult(_document.Id);
    }
}
