using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class GenAiConversationHandler(ServerStore server, DocumentDatabase database, GenAiConfiguration configuration) : ConversationHandler(server, database)
{
    private readonly DocumentDatabase _database = database;

    public async Task<GenAiHandlerResult> HandleRequest(CancellationToken token)
    {
        using var _ = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        // the parameters is a BlittableJsonReaderObject which use a shared underlying context that is not thread safe, so we need to clone it for concurrent read
        _request.Parameters = _request.Parameters?.CloneForConcurrentRead(context);

        var response = await HandleRequest(context, token);
        var result = new GenAiHandlerResult
        {
            Response = response.Response.ToString(), 
            Usage = response.Usage, 
        };

        if (configuration.TestMode)
            result.ConversationDocument = _document.ToBlittable(context).ToString();
                
        return result;
    }

    protected override Task<string> TryPersistAsync(JsonOperationContext context, List<BlittableJsonReaderObject> historyDocs)
    {
        if (configuration.EnableTracing == false || configuration.TestMode)
            return Task.FromResult(_document.Id);

        return base.TryPersistAsync(context, null);
    }
}

public class GenAiHandlerResult
{
    public string Response;

    public AiUsage Usage;

    public string ConversationDocument;
}
