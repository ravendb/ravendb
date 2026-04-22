using Raven.Client.Extensions;
using Newtonsoft.Json;
using Sparrow.Json.Sync;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Sparrow.Json;
using System.IO;
using Raven.Client.Documents.Commands.Batches;

namespace Raven.Client.Documents.AI;

internal class AiConversation : IAiConversationOperations
{
    private readonly AiOperations _aiOperations;
    private readonly string _agentId;
    private readonly AiConversationCreationOptions _options;

    private string _conversationId;
    private List<AiAgentActionRequest> _actionRequests;
    private readonly Dictionary<string, AiAgentActionResponse> _actionResponses = [];
    private readonly List<AiAgentArtificialActionResponse> _artificialActions = [];
    private readonly List<ContentPart> _promptParts = [];
    private string _changeVector;
    private readonly List<ICommandData> _attachmentsCommands = new();
    private StringBuilder _jsonBuffer;
    private StringWriter _jsonWriter;
    
    public string ChangeVector => _changeVector;

    private delegate Task HandleActionDelegate(JsonOperationContext context, AiAgentActionRequest actionRequest, CancellationToken token);

    private readonly Dictionary<string, HandleActionDelegate> _invocations = new();
    private readonly HashSet<string> _dispatchedToolIds = new();

    public AiConversation(AiOperations aiOperations, string agentId, string conversationId, AiConversationCreationOptions options, string changeVector)
    {
        ValidationMethods.AssertNotNullOrEmpty(aiOperations, nameof(aiOperations));
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));

        _aiOperations = aiOperations;
        _agentId = agentId;
        _conversationId = conversationId;
        _options = options;
        _changeVector = changeVector;
    }
    
    public void AddAttachment(string name, Stream stream, string contentType)
    {
        if (stream == null)
            throw new ArgumentNullException(nameof(stream));

        var attachmentName = name;
        _attachmentsCommands.Add(new PutAttachmentCommandData("__this__", attachmentName, stream, contentType, changeVector: null));
    }

    public void CopyAttachmentFrom(string sourceDocumentId, string fileName)
    {
        ValidationMethods.AssertNotNullOrEmpty(sourceDocumentId, nameof(sourceDocumentId));
        ValidationMethods.AssertNotNullOrEmpty(fileName, nameof(fileName));
        ValidationMethods.AssertNotNullOrEmpty(sourceDocumentId, nameof(sourceDocumentId));

        _attachmentsCommands.Add(new CopyAttachmentCommandData(sourceDocumentId, fileName, "__this__", fileName, changeVector: null));
    }

    public IEnumerable<AiAgentActionRequest> RequiredActions() =>
        _actionRequests ?? throw new InvalidOperationException($"You have to call {nameof(Run)}/{nameof(RunAsync)} first");

    public string Id
    {
        get
        {
            if (_conversationId == null || _conversationId.EndsWith("/") || _conversationId.EndsWith("|"))
            {
                throw new InvalidOperationException($"This is a new conversation, the ID wasn't set yet, you have to call {nameof(Run)}/{nameof(RunAsync)}");
            }

            return _conversationId;
        }
    }
    
    public void AddArtificialActionWithResponse(string toolId, string actionResponse)
    {
        ValidationMethods.AssertNotNullOrEmpty(toolId, nameof(toolId));
        ValidationMethods.AssertNotNullOrEmpty(actionResponse, nameof(actionResponse));

        _artificialActions.Add(new AiAgentArtificialActionResponse
        {
            ToolId = toolId,
            Content = actionResponse,
        });
    }

    public void AddArtificialActionWithResponse<TResponse>(string toolId, TResponse actionResponse) where TResponse : class
    {
        ValidationMethods.AssertNotNullOrEmpty(toolId, nameof(toolId));
        if (actionResponse == null)
            throw new ArgumentNullException(nameof(actionResponse), $"Action response for '{toolId}' cannot be null.");

        if (actionResponse is string str)
        {
            AddArtificialActionWithResponse(toolId, str);
            return;
        }

        AddArtificialActionWithResponse(toolId, SerializeToJson(actionResponse));
    }

    public void AddActionResponse<TResponse>(string toolId, TResponse actionResponse) where TResponse : class
    {
        ValidationMethods.AssertNotNullOrEmpty(toolId, nameof(toolId));
        if (actionResponse == null)
            throw new ArgumentNullException(nameof(actionResponse), $"Action response for '{toolId}' cannot be null.");

        if (actionResponse is string str)
        {
            AddActionResponse(toolId, str);
            return;
        }

        AddActionResponse(toolId, SerializeToJson(actionResponse));
    }

    public void AddActionResponse(string toolId, string actionResponse)
    {
        ValidationMethods.AssertNotNullOrEmpty(toolId, nameof(toolId));
        ValidationMethods.AssertNotNullOrEmpty(actionResponse, nameof(actionResponse));

        if (_actionResponses.ContainsKey(toolId))
            throw new InvalidOperationException($"An action response for tool-id '{toolId}' was already added. Each tool call must have exactly one response. " +
                                                $"If you're using {nameof(Handle)}, return the value from the handler (don't call {nameof(AddActionResponse)} manually).");

        _actionResponses[toolId] = new AiAgentActionResponse
        {
            ToolId = toolId,
            Content = actionResponse
        };
    }

    public void SetUserPrompt(string userPrompt)
    {
        ValidationMethods.AssertNotNullOrEmpty(userPrompt, nameof(userPrompt));
        _promptParts.Clear();
        AddUserPrompt(userPrompt);
    }

    public void AddUserPrompt(params IEnumerable<string> prompts)
    {
        foreach (string prompt in prompts)
        {
            ValidationMethods.AssertNotNullOrEmpty(prompt, nameof(prompt));
            _promptParts.Add(new TextPart(prompt));
        }
    }

    public void Handle<TArgs, TResult>(string actionName, Func<TArgs, Task<TResult>> action, AiHandleErrorStrategy aiHandleError) 
        where TArgs : class 
        where TResult : class
    {
        Handle<TArgs, TResult>(actionName, (_, token) => action(token), aiHandleError);
    }

    public void Handle<TArgs>(string actionName, Func<TArgs, object> action, AiHandleErrorStrategy aiHandleError) where TArgs : class
    {
        Handle<TArgs>(actionName, (_, token) => action(token), aiHandleError);
    }

    public void Handle<TArgs, TResult>(string actionName, Func<AiAgentActionRequest, TArgs, Task<TResult>> action, AiHandleErrorStrategy aiHandleError)
        where TArgs : class
        where TResult : class
    {
        Receive<TArgs>(actionName, async (request, args) =>
        {
            var result = await action(request, args).ConfigureAwait(false);
            AddActionResponse(request.ToolId, result);
        }, aiHandleError);
    }

    public void Handle<TArgs>(string actionName, Func<AiAgentActionRequest, TArgs, object> action, AiHandleErrorStrategy aiHandleError) where TArgs : class
    {
        Receive<TArgs>(actionName, (request, args) =>
        {
            var result = action(request, args);
            AddActionResponse(request.ToolId, result);
            return Task.CompletedTask;
        }, aiHandleError);
    }

    public void Receive<TArgs>(string actionName, Func<AiAgentActionRequest, TArgs, Task> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel) where TArgs : class
    {
        var t = new AiActionContext<TArgs>(this, action, aiHandleError);
        AddAction(actionName, t.ExecuteAsync);
    }

    public void Receive<TArgs>(string actionName, Action<AiAgentActionRequest, TArgs> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel) where TArgs : class
    {
        var t = new AiActionContext<TArgs>(this, (request, args) =>
        {
            action(request, args);
            return Task.CompletedTask;
        }, aiHandleError);
        AddAction(actionName, t.ExecuteAsync);
    }

    private void AddAction(string actionName, HandleActionDelegate t)
    {
        if (_invocations.ContainsKey(actionName))
            throw new InvalidOperationException($"Action '{actionName}' already exists.");

        _invocations.Add(actionName, t);
    }

    public AiAnswer<TAnswer> Run<TAnswer>() => AsyncHelpers.RunSync(() => RunAsync<TAnswer>());

    public Task<AiAnswer<TAnswer>> StreamAsync<TAnswer>(Expression<Func<TAnswer, string>> streamPropertyPath, Func<string, Task> streamedChunksCallback, CancellationToken token = default)
    {
        return StreamAsync<TAnswer>(streamPropertyPath.ToPropertyPath(_aiOperations._store.Conventions), streamedChunksCallback, token);
    }

    public async Task<AiAnswer<TAnswer>> StreamAsync<TAnswer>(string streamPropertyPath, Func<string, Task> streamedChunksCallback, CancellationToken token = default)
    {
        while (true)
        {
            var r = await RunAsyncInternal<TAnswer>(streamPropertyPath, streamedChunksCallback, token).ConfigureAwait(false);
            if (await HandleServerReplyAsync(r, token).ConfigureAwait(false))
                return r;
        }
    }

    public async Task<AiAnswer<TAnswer>> RunAsync<TAnswer>(CancellationToken token = default)
    {
        _dispatchedToolIds.Clear();

        while (true)
        {
            var r = await RunAsyncInternal<TAnswer>(streamPropertyPath: null, streamedChunksCallback: null, token).ConfigureAwait(false);
            if (await HandleServerReplyAsync(r, token).ConfigureAwait(false))
                return r;
        }
    }

    private async Task<bool> HandleServerReplyAsync<TAnswer>(AiAnswer<TAnswer> r, CancellationToken token)
    {
        if (r.Status == AiConversationResult.Done)
            return true;

        if (_actionRequests.Count == 0)
            throw new InvalidOperationException($"There are no action requests to process, but Status was {r.Status}, should not be possible.");

        using (_aiOperations.AllocateOperationContext(out JsonOperationContext ctx))
        {
            foreach (var action in _actionRequests)
            {
                if (_dispatchedToolIds.Add(action.ToolId) == false)
                    continue;

                if (_invocations.TryGetValue(action.Name, out var invocation))
                {
                    // error handling here is expected to be done by the invocation based on the error strategy the user choose
                    await invocation.Invoke(ctx, action, token).ConfigureAwait(false);
                }
                else if (OnUnhandledAction is { } onUnhandledAction)
                {
                    await onUnhandledAction(new UnhandledActionEventArgs(this, action, token)).ConfigureAwait(false);
                }
                else
                {
                    throw new InvalidOperationException(
                        $"There is no action defined for action '{action.Name}' on agent '{_agentId}' ({_conversationId}), but it was invoked by the model with: {action.Arguments}. " +
                        $"Did you forget to call {nameof(Receive)} or {nameof(Handle)}? You can also handle unexpected action invocations using the {nameof(OnUnhandledAction)} event.");
                }
            }
        }

        // We have nothing to tell the server, but still have action requests pending
        // we need to send those action requests to the caller that can handle them
        return _actionResponses.Count == 0;
    }

    public event Func<UnhandledActionEventArgs, Task> OnUnhandledAction;

    private async Task<AiAnswer<TAnswer>> RunAsyncInternal<TAnswer>(string streamPropertyPath, Func<string, Task> streamedChunksCallback, CancellationToken token = default)
    {
        if (
            // if this is null, it is the first time we call RunAsync, so we are going to the server to get the pending actions
            _actionRequests is { Count: 0 } &&
            // otherwise, we already went to the server and have nothing new to tell it, so we are done
            _promptParts.Count == 0 && _actionResponses.Count == 0 && _attachmentsCommands.Count == 0)
        {
            return new AiAnswer<TAnswer>
            {
                Status = AiConversationResult.Done
            };
        }
        var op = new RunConversationOperation<TAnswer>(_agentId, _conversationId, _promptParts, [.. _actionResponses.Values], _artificialActions, _options, _changeVector, _attachmentsCommands, streamPropertyPath, streamedChunksCallback);

        try
        {
            var r = await _aiOperations._executor.SendAsync(op, token).ConfigureAwait(false);
            _changeVector = r.ChangeVector;
            _conversationId = r.ConversationId;
            _actionRequests = r.ActionRequests ?? [];

            return new AiAnswer<TAnswer>
            {
                Answer = r.Response,
                Status = _actionRequests.Count > 0 ? AiConversationResult.ActionRequired : AiConversationResult.Done,
                Usage = r.Usage,
                Elapsed = r.Elapsed
            };
        }
        catch (ConcurrencyException e)
        {
            _changeVector = e.ActualChangeVector;
            throw;
        }
        finally
        {
            // clear the user prompt and tool responses after running the conversation
            _promptParts.Clear();
            _actionResponses.Clear();
            _artificialActions.Clear();
            _attachmentsCommands.Clear();
        }
    }

    private string SerializeToJson(object value)
    {
        _jsonBuffer ??= new StringBuilder();
        _jsonWriter ??= new StringWriter(_jsonBuffer);
        _jsonBuffer.Clear();
        var serializer = (JsonSerializer)_aiOperations._store.Conventions.Serialization.CreateSerializer();
        serializer.Serialize(_jsonWriter, value);
        return _jsonBuffer.ToString();
    }

    internal class AiActionContext<TActionParametersSchema>
        where TActionParametersSchema : class
    {
        private readonly AiConversation _conversation;
        private readonly Func<AiAgentActionRequest, TActionParametersSchema, Task> _asyncAction;
        private readonly AiHandleErrorStrategy _errorStrategy;

        public AiActionContext(AiConversation conversation, Func<AiAgentActionRequest, TActionParametersSchema, Task> asyncAction, AiHandleErrorStrategy errorStrategy)
        {
            _conversation = conversation;
            _asyncAction = asyncAction;
            _errorStrategy = errorStrategy;
        }

        public async Task ExecuteAsync(JsonOperationContext context, AiAgentActionRequest actionRequest, CancellationToken token = default)
        {
            if (typeof(TActionParametersSchema) == typeof(string))
            {
                var args = actionRequest.Arguments as TActionParametersSchema;
                await Invoke(actionRequest, args).ConfigureAwait(false);
                return;
            }

            using (var json = context.Sync.ReadForMemory(actionRequest.Arguments, "tool/arguments"))
            {
                var converter = _conversation._aiOperations._store.Conventions.Serialization.DefaultConverter;
                var args = converter.FromBlittable<TActionParametersSchema>(json);

                await Invoke(actionRequest, args).ConfigureAwait(false);
            }
        }

        private async Task Invoke(AiAgentActionRequest actionRequest, TActionParametersSchema args)
        {
            try
            {
                await _asyncAction.Invoke(actionRequest, args).ConfigureAwait(false);
            }
            catch (Exception e) when (_errorStrategy is AiHandleErrorStrategy.SendErrorsToModel)
            {
                _conversation.AddActionResponse(actionRequest.ToolId, CreateErrorMessageForLlm(e));
            }
        }

        internal static string CreateErrorMessageForLlm(Exception e)
        {
            var sb = new StringBuilder();
            var currentException = e;
            int indentLevel = 0;

            //AiException: AI model processing failed
            //  HttpRequestException: Request to AI service failed
            //    TaskCanceledException: A task was canceled
            while (currentException != null)
            {
                if (indentLevel > 0)
                    sb.Append(Environment.NewLine);

                for (int i = 0; i < indentLevel; i++)
                    sb.Append("  ");

                sb.Append(currentException.GetType().Name);
                sb.Append(": ");
                sb.Append(currentException.Message);

                currentException = currentException.InnerException;
                indentLevel++;
            }

            return sb.ToString();
        }
    }
}
