using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
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
    private readonly IEnumerable<ContentPart> _promptParts;
    private readonly AiConversationCreationOptions _options;

    private readonly string _conversationId;
    private readonly List<AiAgentActionResponse> _actionResponses;
    private readonly string _changeVector;

    private readonly string _streamPropertyPath;
    private readonly Func<string, Task> _streamedChunksCallback;

    public RunConversationOperation(
        string agentId,
        string conversationId,
        List<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        AiConversationCreationOptions options,
        string changeVector) : this(agentId, conversationId, promptParts, actionResponses, options, changeVector, null, null)
    {
    }

    public RunConversationOperation(
        string agentId,
        string conversationId,
        IEnumerable<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        AiConversationCreationOptions options,
        string changeVector,
        string streamPropertyPath,
        Func<string, Task> streamedChunksCallback)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));
        PortableExceptions.ThrowIfNot<InvalidOperationException>(streamPropertyPath is null == streamedChunksCallback is null,
            "Both streamPropertyPath and streamedChunksCallback must be specified together");

        _agentId = agentId;
        _conversationId = conversationId;
        _promptParts = promptParts;
        _changeVector = changeVector;
        _actionResponses = actionResponses;
        _options = options;

        _streamPropertyPath = streamPropertyPath;
        _streamedChunksCallback = streamedChunksCallback;
    }

    [Obsolete("Use the constructor that accepts a List or an Array instead. This is for backward compatibility.", error: false)]
    public RunConversationOperation(
            string agentId,
            string conversationId,
            string userPrompt,
            List<AiAgentActionResponse> actionResponses,
            AiConversationCreationOptions options,
            string changeVector,
            string streamPropertyPath,
            Func<string, Task> streamedChunksCallback)
        : this(agentId, conversationId, new List<ContentPart> { new TextPart(userPrompt) }, actionResponses, options, changeVector, streamPropertyPath, streamedChunksCallback)
    {
    }
    public virtual RavenCommand<ConversationResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new RunConversationOperationCommand(this, conventions);
    }

    internal class RunConversationOperationCommand : RavenCommand<ConversationResult<TSchema>>, IRaftCommand
    {
        private readonly RunConversationOperation<TSchema> _parent;
        private readonly DocumentConventions _conventions;

        public RunConversationOperationCommand(RunConversationOperation<TSchema> parent, DocumentConventions conventions)
        {
            _conventions = conventions;
            _parent = parent;

            if (parent._streamPropertyPath is not null)
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

            if (_parent._streamPropertyPath is not null)
                url += $"&streaming=true&streamPropertyPath={Uri.EscapeDataString(_parent._streamPropertyPath)}";

            var body = new ConversionRequestBody
            {
                ActionResponses = _parent._actionResponses,
                UserPrompt = _parent._promptParts,
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
            using var streamReader = new StreamReader(stream);
            while (true)
            {
                var line = await streamReader.ReadLineAsync().ConfigureAwait(false);
                if (line is null)
                    break;

                if (line.StartsWith("{"))
                {
                    using var final = context.Sync.ReadForMemory(line, "final/result");
                    SetResponse(context, final, fromCache: false);
                    break;
                }

                string unescaped = JToken.Parse(line).Value<string>();
                await _parent._streamedChunksCallback(unescaped).ConfigureAwait(false);
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
