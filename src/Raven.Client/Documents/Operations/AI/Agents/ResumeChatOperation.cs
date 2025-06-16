using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class ResumeChatOperation<TSchema> : IMaintenanceOperation<ChatResult<TSchema>> where TSchema : new()
{
    private readonly string _name;
    private readonly string _chatId;
    private readonly string _userPrompt;
    private readonly List<ToolResponse> _toolResponses;

    public ResumeChatOperation(string agentName, string chatId, string userPrompt = null, List<ToolResponse> toolResponses = null)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentName, nameof(agentName));
        ValidationMethods.AssertNotNullOrEmpty(chatId, nameof(chatId));

        if (string.IsNullOrEmpty(userPrompt) && (toolResponses == null || toolResponses.Count == 0))
            throw new ArgumentException($"Either '{nameof(userPrompt)}' or '{nameof(toolResponses)}' must be provided to resume a chat.");

        _name = agentName;
        _chatId = chatId;
        _userPrompt = userPrompt;
        _toolResponses = toolResponses;
    }

    public RavenCommand<ChatResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new ResumeChatOperationCommand(_name, _chatId, _userPrompt, _toolResponses, conventions);
    }

    internal sealed class ResumeChatOperationCommand : RavenCommand<ChatResult<TSchema>>
    {
        private readonly string _name;
        private readonly string _chatId;
        private readonly string _userPrompt;
        private readonly List<ToolResponse> _toolResponses;
        private readonly DocumentConventions _conventions;

        public ResumeChatOperationCommand(string name, string chatId, string userPrompt, List<ToolResponse> toolResponses, DocumentConventions conventions)
        {
            _name = name;
            _chatId = chatId;
            _userPrompt = userPrompt;
            _toolResponses = toolResponses;
            _conventions = conventions;
        }
        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/resume?name={Uri.EscapeDataString(_name)}&chatId={Uri.EscapeDataString(_chatId)}";
            var body = new ResumeChatBody { ToolResponse = _toolResponses, UserPrompt = _userPrompt};

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(),"resume-chat-params")).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = ChatResult<TSchema>.Convert(response, _conventions);
        }
    }
}
