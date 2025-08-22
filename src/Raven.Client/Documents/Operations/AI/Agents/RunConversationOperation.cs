using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.ServerSentEvents;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class RunConversationOperation<TSchema> : IMaintenanceOperation<ConversationResult<TSchema>>
{
    private readonly string _agentId;
    private readonly string _userPrompt;
    private readonly AiConversationCreationOptions _options;

    private readonly string _conversationId;
    private readonly List<AiAgentActionResponse> _actionResponses;
    private readonly string _changeVector;
    private readonly string _propertyToStream;
    private readonly Func<string, Task> _streamedChunksCallback;

    public RunConversationOperation(
        string agentId,
        string conversationId,
        string userPrompt,
        List<AiAgentActionResponse> actionResponses,
        AiConversationCreationOptions options,
        string changeVector) : this(agentId, conversationId, userPrompt, actionResponses, options, changeVector, null, null)
    {
    }

    public RunConversationOperation(
        string agentId,
        string conversationId,
        string userPrompt,
        List<AiAgentActionResponse> actionResponses,
        AiConversationCreationOptions options,
        string changeVector,
        string propertyToStream,
        Func<string, Task> streamedChunksCallback)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));
        PortableExceptions.ThrowIfNot<InvalidOperationException>(propertyToStream is null == streamedChunksCallback is null,
            "Both propertyToStream and streamedChunksCallback must be specified together");

        _agentId = agentId;
        _conversationId = conversationId;
        _userPrompt = userPrompt;
        _changeVector = changeVector;
        _actionResponses = actionResponses;
        _options = options;


        _propertyToStream = propertyToStream;
        _streamedChunksCallback = streamedChunksCallback;
    }

    public RavenCommand<ConversationResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new RunConversationOperationCommand(this, conventions);
    }

    internal sealed class RunConversationOperationCommand : RavenCommand<ConversationResult<TSchema>>, IRaftCommand
    {
        private readonly RunConversationOperation<TSchema> _parent;
        private readonly DocumentConventions _conventions;

        public RunConversationOperationCommand(RunConversationOperation<TSchema> parent, DocumentConventions conventions)
        {
            _conventions = conventions;
            _parent = parent;
            if (parent._propertyToStream is not null)
            {
                ResponseType = RavenCommandResponseType.Raw;
            }
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent" +
                  $"?conversationId={Uri.EscapeDataString(_parent._conversationId)}&agentId={Uri.EscapeDataString(_parent._agentId)}";

            if (_parent._conversationId[_parent._conversationId.Length - 1] == '|')
            {
                _raftId = Guid.NewGuid().ToString();
            }

            if (_parent._changeVector != null)
                url += $"&changeVector={Uri.EscapeDataString(_parent._changeVector)}";

            if (_parent._propertyToStream is not null)
                url += $"&streaming=true&propertyToStream={Uri.EscapeDataString(_parent._propertyToStream)}";

            var body = new ConversionRequestBody
            {
                ActionResponses = _parent._actionResponses,
                UserPrompt = _parent._userPrompt,
                CreationOptions = _parent._options
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(), "conversation-params")).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override async Task SetResponseRawAsync(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            await foreach (var msg in SseParser.Create(stream).EnumerateAsync())
            {
                if (msg.EventType is "result")
                {
                    var final = context.Sync.ReadForMemory(msg.Data, "final/result");
                    Result = ConversationResult<TSchema>.Convert(final, _conventions);
                    break;
                }

                // streaming the output...
                await _parent._streamedChunksCallback(msg.Data).ConfigureAwait(false);
            }
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = ConversationResult<TSchema>.Convert(response, _conventions);
        }

        private string _raftId = string.Empty;
        public string RaftUniqueRequestId => _raftId;
    }
}
