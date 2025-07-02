using System.Threading.Tasks;
using Sparrow.Json;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForResumeChat : AbstractSingleAiAgentProcessor
{
    public AiAgentProcessorForResumeChat([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var chatId = RequestHandler.GetStringQueryString("chatId", required: true);

        using var _ = RequestHandler.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var __ = context.OpenReadTransaction();

        var chat = RequestHandler.Database.DocumentsStorage.Get(context, chatId);
        if (chat == null)
            throw new DocumentDoesNotExistException(chatId);

        var chatDocument = ChatDocument.ToDocument(chatId, chat.Data);
        var body = await ReadResumeChatBodyAsync(context, token.Token);

        var configuration = GetAiAgentConfiguration(chatDocument.Agent);

        AddNewMessages(context, body, chatDocument);

        var r = await TalkAsync(context, configuration, chatDocument, token: token);

        await TryPersistAsync(configuration, chatId, r.Document);

        await WriteResponseAsync(context, chatId, r);
    }

    void AddNewMessages(DocumentsOperationContext context, (BlittableJsonReaderArray ActionResponse, string UserPrompt) body, ChatDocument chatDocument)
    {
        if (string.IsNullOrEmpty(body.UserPrompt) == false)
        {
            chatDocument.AddMessage(context, context.ReadObject(new DynamicJsonValue
            {
                ["role"] = "user",
                ["content"] = body.UserPrompt
            }, "user/msg"));
        }

        if (body.ActionResponse != null)
        {
            foreach (BlittableJsonReaderObject tool in body.ActionResponse)
            {
                var t = JsonDeserializationClient.ToolResponse(tool);
                chatDocument.Messages.Add(context.ReadObject(new DynamicJsonValue
                {
                    ["tool_call_id"] = t.ToolId,
                    ["role"] = "tool",
                    ["content"] = t.Content
                }, "user/tool"));
            }
        }
    }
}
