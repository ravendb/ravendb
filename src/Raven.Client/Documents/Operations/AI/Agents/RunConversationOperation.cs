using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Continues or resumes a conversation with an AI agent, returning a typed response and tool call requests.
/// </summary>
public class RunConversationOperation<TSchema> : IMaintenanceOperation<ConversationResult<TSchema>>
{
    private readonly string _agentId;
    private readonly IEnumerable<ContentPart> _promptParts;
    private readonly AiConversationCreationOptions _options;

    private readonly string _conversationId;
    private readonly List<AiAgentActionResponse> _actionResponses;
    private readonly List<AiAgentArtificialActionResponse> _artificialActions;
    private readonly string _changeVector;

    private readonly string _streamPropertyPath;
    private readonly Func<string, Task> _streamedChunksCallback;
    private readonly List<ICommandData> _attachmentsCommands;
    private readonly bool? _enableFullDebug;

    /// <summary>
    /// Initializes a new conversation step for the specified agent and conversation.
    /// </summary>
    /// <param name="agentId">The agent identifier to route this conversation to.</param>
    /// <param name="conversationId">The conversation document ID used to maintain state.</param>
    /// <param name="userPrompt">The user's prompt to send to the model.</param>
    /// <param name="actionResponses">Optional responses for tool action requests from a previous step.</param>
    /// <param name="options">Creation options including conversation expiration and tool parameters.</param>
    /// <param name="changeVector">Optional expected change vector for optimistic concurrency on the conversation document.</param>
    public RunConversationOperation(
        string agentId,
        string conversationId,
        List<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        AiConversationCreationOptions options,
        string changeVector) : this(agentId, conversationId, promptParts, actionResponses, [], options, changeVector, null, null)
    {
    }

    /// <summary>
    /// Initializes a new conversation step, including artificial actions injected into the conversation flow.
    /// </summary>
    /// <param name="agentId">The agent identifier to route this conversation to.</param>
    /// <param name="conversationId">The conversation document ID used to maintain state.</param>
    /// <param name="promptParts">The parts of the user prompt to send to the model.</param>
    /// <param name="actionResponses">Optional responses for tool action requests from a previous step.</param>
    /// <param name="artificialActions">Manually injected responses to tool calls.</param>
    /// <param name="options">Creation options including conversation expiration and tool parameters.</param>
    /// <param name="changeVector">Optional expected change vector for optimistic concurrency on the conversation document.</param>
    public RunConversationOperation(
        string agentId,
        string conversationId,
        List<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        List<AiAgentArtificialActionResponse> artificialActions,
        AiConversationCreationOptions options,
        string changeVector) : this(agentId, conversationId, promptParts, actionResponses, artificialActions, options, changeVector, null, null)
    {
    }

    /// <summary>
    /// Initializes a new conversation step with support for streaming the response.
    /// </summary>
    /// <param name="agentId">The agent identifier to route this conversation to.</param>
    /// <param name="conversationId">The conversation document ID used to maintain state.</param>
    /// <param name="promptParts">The parts of the user prompt to send to the model.</param>
    /// <param name="actionResponses">Optional responses for tool action requests from a previous step.</param>
    /// <param name="options">Creation options including conversation expiration and tool parameters.</param>
    /// <param name="changeVector">Optional expected change vector for optimistic concurrency on the conversation document.</param>
    /// <param name="streamPropertyPath">The JSON path of the property to stream back to the client.</param>
    /// <param name="streamedChunksCallback">The callback function invoked when a new chunk of streamed data arrives.</param>
    public RunConversationOperation(
        string agentId,
        string conversationId,
        List<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        AiConversationCreationOptions options,
        string changeVector,
        string streamPropertyPath,
        Func<string, Task> streamedChunksCallback) : this(agentId, conversationId, promptParts, actionResponses, [], options, changeVector, streamPropertyPath, streamedChunksCallback)
    {
    }

    /// <summary>
    /// Initializes a new conversation step with full control over artificial actions and streaming.
    /// </summary>
    /// <param name="agentId">The agent identifier to route this conversation to.</param>
    /// <param name="conversationId">The conversation document ID used to maintain state.</param>
    /// <param name="promptParts">The parts of the user prompt to send to the model.</param>
    /// <param name="actionResponses">Optional responses for tool action requests from a previous step.</param>
    /// <param name="artificialActions">Manually injected responses to tool calls.</param>
    /// <param name="options">Creation options including conversation expiration and tool parameters.</param>
    /// <param name="changeVector">Optional expected change vector for optimistic concurrency on the conversation document.</param>
    /// <param name="streamPropertyPath">The JSON path of the property to stream back to the client.</param>
    /// <param name="streamedChunksCallback">The callback function invoked when a new chunk of streamed data arrives.</param>
    public RunConversationOperation(string agentId,
        string conversationId,
        IEnumerable<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        List<AiAgentArtificialActionResponse> artificialActions,
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
        _artificialActions = artificialActions;
        _options = options;

        _streamPropertyPath = streamPropertyPath;
        _streamedChunksCallback = streamedChunksCallback;
    }

    public RunConversationOperation(string agentId,
        string conversationId,
        IEnumerable<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        List<AiAgentArtificialActionResponse> artificialActions,
        AiConversationCreationOptions options,
        string changeVector,
        List<ICommandData> attachmentsCommands,
        string streamPropertyPath,
        Func<string, Task> streamedChunksCallback)
        : this(agentId, conversationId, promptParts, actionResponses, artificialActions, options, changeVector, streamPropertyPath, streamedChunksCallback)
    {
        _attachmentsCommands = attachmentsCommands;
    }

    internal RunConversationOperation(string agentId,
        string conversationId,
        IEnumerable<ContentPart> promptParts,
        List<AiAgentActionResponse> actionResponses,
        List<AiAgentArtificialActionResponse> artificialActions,
        AiConversationCreationOptions options,
        string changeVector,
        List<ICommandData> attachmentsCommands,
        string streamPropertyPath,
        Func<string, Task> streamedChunksCallback,
        bool? enableFullDebug)
        : this(agentId, conversationId, promptParts, actionResponses, artificialActions, options, changeVector, attachmentsCommands, streamPropertyPath, streamedChunksCallback)
    {
        _enableFullDebug = enableFullDebug;
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
        : this(agentId, conversationId, new List<ContentPart>
        {
            new TextPart(userPrompt)
        }, actionResponses, [], options, changeVector, streamPropertyPath, streamedChunksCallback)
    {
    }

    /// <summary>
    /// Creates the command that will be sent to the server to execute the conversation step.
    /// </summary>
    public virtual RavenCommand<ConversationResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new RunConversationOperationCommand(this, conventions);
    }

    internal class RunConversationOperationCommand : RavenCommand<ConversationResult<TSchema>>, IRaftCommand
    {
        private readonly RunConversationOperation<TSchema> _parent;
        private readonly DocumentConventions _conventions;
        private List<Stream> _attachmentStreams;
        private HashSet<Stream> _uniqueAttachmentStreams;

        public RunConversationOperationCommand(RunConversationOperation<TSchema> parent, DocumentConventions conventions)
        {
            _conventions = conventions;
            _parent = parent;

            if (parent._streamPropertyPath is not null)
            {
                ResponseType = RavenCommandResponseType.Raw;
            }

            if (_parent._conversationId.EndsWith("|"))
            {
                _raftId = Guid.NewGuid().ToString();
            }

            if (_parent._attachmentsCommands != null)
            {
                foreach (var command in _parent._attachmentsCommands)
                {
                    if (command is PutAttachmentCommandData put)
                    {
                        _attachmentStreams ??= new List<Stream>();
                        _uniqueAttachmentStreams ??= new HashSet<Stream>();

                        var stream = put.Stream;

                        PutAttachmentCommandHelper.TryValidateStream(stream, put.RemoteParameters);

                        if (_uniqueAttachmentStreams.Add(stream) == false)
                            PutAttachmentCommandHelper.ThrowStreamWasAlreadyUsed();

                        _attachmentStreams.Add(stream);
                    }
                }
            }
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent" +
                  $"?conversationId={Uri.EscapeDataString(_parent._conversationId)}&agentId={Uri.EscapeDataString(_parent._agentId)}";

            if (_parent._changeVector != null)
                url += $"&changeVector={Uri.EscapeDataString(_parent._changeVector)}";

            if (_parent._streamPropertyPath is not null)
                url += $"&streaming=true&streamPropertyPath={Uri.EscapeDataString(_parent._streamPropertyPath)}";

            if (_parent._enableFullDebug.HasValue)
                url += $"&enableFullDebug={(_parent._enableFullDebug.Value ? "true" : "false")}";

            var body = new ConversionRequestBody
            {
                ActionResponses = _parent._actionResponses,
                ArtificialActions = _parent._artificialActions,
                UserPrompt = _parent._promptParts,
                CreationOptions = _parent._options,
                AttachmentCommands = _parent._attachmentsCommands
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(), "conversation-params")).ConfigureAwait(false);
                }, _conventions)
            };

            if (_parent._attachmentsCommands != null)
            {
                var commandsAsBlittable = new BlittableJsonReaderObject[_parent._attachmentsCommands.Count];
                for (var i = 0; i < _parent._attachmentsCommands.Count; i++)
                {
                    var command = _parent._attachmentsCommands[i];
                    var json = command.ToJson(_conventions, ctx);
                    commandsAsBlittable[i] = ctx.ReadObject(json, "command");
                }

                var multipartContent = new MultipartContent { request.Content };
                multipartContent.Add(new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("Commands", commandsAsBlittable);
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                );

                if (_attachmentStreams is { Count: > 0 })
                {
                    foreach (var stream in _attachmentStreams)
                    {
                        PutAttachmentCommandHelper.PrepareStream(stream);
                        var streamContent = new AttachmentStreamContent(stream, CancellationToken);
                        streamContent.Headers.TryAddWithoutValidation(Constants.Headers.CommandType, Constants.Headers.AttachmentStream);
                        multipartContent.Add(streamContent);
                    }
                }

                request.Content = multipartContent;
            }

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
