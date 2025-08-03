using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Client.Documents.AI;

internal class AiConversation : IAiConversationOperations
{
    private readonly AiOperations _aiOperations;
    private readonly string _agentId;
    private readonly AiConversationCreationOptions _options;

    private string _conversationId;
    private List<AiAgentActionRequest> _actionRequests;
    private List<AiAgentActionResponse> _actionResponses = [];
    private string _userPrompt;
    private string _changeVector;
    public string ChangeVector => _changeVector;
    private readonly Dictionary<string, IAiActionContext> _invocations = new();

    public AiConversation(AiOperations aiOperations, string agentId, string conversationId, AiConversationCreationOptions options, string changeVector)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));

        _aiOperations = aiOperations;
        _agentId = agentId;
        _conversationId = conversationId;
        _options = options;
        _changeVector = changeVector;
    }

    public IEnumerable<AiAgentActionRequest> RequiredActions() =>
        _actionRequests ?? throw new InvalidOperationException($"You have to call {nameof(Run)}/{nameof(RunAsync)} first");

    public string Id => _conversationId ??
                        throw new InvalidOperationException($"This is a new conversation, the ID wasn't set yet, you have to call {nameof(Run)}/{nameof(RunAsync)}");

    public void AddActionResponse<TResponse>(string actionId, TResponse actionResponse) where TResponse : class
    {
        if (actionResponse is string str)
        {
            AddActionResponse(actionId, str);
            return;
        }

        using (_aiOperations.AllocateOperationContext(out var context))
        {
            var jsonSerializer = _aiOperations._store.Conventions.Serialization.DefaultConverter;
            var json = jsonSerializer.ToBlittable(actionResponse, context);
            AddActionResponse(actionId, json.ToString());
        }
    }

    public void AddActionResponse(string actionId, string actionResponse)
    {
        _actionResponses.Add(new AiAgentActionResponse
        {
            ToolId = actionId, 
            Content = actionResponse
        });
    }

    public void SetUserPrompt(string userPrompt)
    {
        ValidationMethods.AssertNotNullOrEmpty(userPrompt, nameof(userPrompt));

        _userPrompt = userPrompt;
    }

    public void Handle<TArgs>(string actionName, Func<TArgs, Task<object>> action, AiHandleErrorStrategy errorStrategy = AiHandleErrorStrategy.Default)
        where TArgs : class
    {
        var t = new AiActionContext<TArgs>(_aiOperations, action, errorStrategy);
        AddAction(actionName, t);
    }

    public void Handle<TArgs>(string actionName, Func<TArgs, object> action, AiHandleErrorStrategy errorStrategy = AiHandleErrorStrategy.Default) where TArgs : class
    {
        var t = new AiActionContext<TArgs>(_aiOperations, action, errorStrategy);
        AddAction(actionName, t);
    }

    private void AddAction(string actionName, IAiActionContext t)
    {
        if (_invocations.ContainsKey(actionName))
            throw new InvalidOperationException($"Action '{actionName}' already exists.");

        _invocations.Add(actionName, t);
    }
  
    public AiAnswer<TAnswer> Run<TAnswer>() => AsyncHelpers.RunSync(() => RunAsync<TAnswer>());

    public async Task<AiAnswer<TAnswer>> RunAsync<TAnswer>(CancellationToken token = default)
    {
        while (true)
        {
            var r = await RunAsyncInternal<TAnswer>(token).ConfigureAwait(false);
            if (r.Status == AiConversationResult.Done) 
                return r;
            
            if (_actionRequests.Count == 0)
                throw new InvalidOperationException($"There are no action requests to process, but Status was {r.Status}, should not be possible.");

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var action in _actionRequests)
                {
                    if (_invocations.TryGetValue(action.Name, out var invocation))
                    {
                        var response = await invocation.ExecuteAsync(ctx, action.Arguments, token).ConfigureAwait(false);
                        AddActionResponse(action.ToolId, response);
                    }
                }
            }

            // we have responses that we need to submit
            if (_actionResponses.Count > 0)
                continue;

            return r;
        }
    }

    private async Task<AiAnswer<TAnswer>> RunAsyncInternal<TAnswer>(CancellationToken token = default)
    {
        // we allow to run the conversation only if it is the first run with no user prompt or tool requests
        // this way we can fetch the pending actions
        if (_actionRequests != null && string.IsNullOrEmpty(_userPrompt) && _actionResponses.Count == 0)
        {
            return new AiAnswer<TAnswer>
            {
                Status = AiConversationResult.Done
            };
        }

        var op = new RunConversationOperation<TAnswer>(_agentId, _conversationId, _userPrompt, _actionResponses, _options, _changeVector);

        try
        {
            var r = await _aiOperations._executor.SendAsync(op, token).ConfigureAwait(false);
            _changeVector = r.ChangeVector;
            _conversationId = r.ConversationId;
            _actionRequests = r.ActionRequests ?? [];

            return new AiAnswer<TAnswer>
            {
                Answer = r.Response, 
                Status = _actionRequests.Count > 0 ? AiConversationResult.ActionRequired : AiConversationResult.Done
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
            _userPrompt = null;
            _actionResponses.Clear();
        }
    }

    private class AiActionContext<TActionParametersSchema> : IAiActionContext where TActionParametersSchema : class
    {
        private readonly Func<TActionParametersSchema, Task<object>> _asyncAction;
        private readonly AiHandleErrorStrategy _errorStrategy;
        private readonly AiOperations _aiOperations;

        public AiActionContext(AiOperations aiOperations, Func<TActionParametersSchema, Task<object>> asyncAction, AiHandleErrorStrategy errorStrategy)
        {
            _asyncAction = asyncAction;
            _errorStrategy = errorStrategy;
            _aiOperations = aiOperations;
        }

        public AiActionContext(AiOperations aiOperations, Func<TActionParametersSchema, object> asyncAction, AiHandleErrorStrategy errorStrategy)
        {
            _asyncAction = args => Task.FromResult(asyncAction(args));
            _aiOperations = aiOperations;
            _errorStrategy = errorStrategy;
        }

        public async Task<object> ExecuteAsync(JsonOperationContext context, string arguments, CancellationToken token = default)
        {
            if (typeof(TActionParametersSchema) == typeof(string))
            {
                var args = arguments as TActionParametersSchema;
                return await Invoke(args).ConfigureAwait(false);
            }

            using (var json = context.Sync.ReadForMemory(arguments, "tool/arguments"))
            {
                var converter = _aiOperations._store.Conventions.Serialization.DefaultConverter;
                var args = converter.FromBlittable<TActionParametersSchema>(json);

                return await Invoke(args).ConfigureAwait(false);
            }
        }

        private async Task<object> Invoke(TActionParametersSchema args)
        {
            try
            {
                return await _asyncAction.Invoke(args).ConfigureAwait(false);
            }
            catch (Exception e) when (_errorStrategy is AiHandleErrorStrategy.SendErrorsToModel or AiHandleErrorStrategy.Default)
            {
                return e;
            }
        }
    }

    private interface IAiActionContext
    {
        Task<object> ExecuteAsync(JsonOperationContext context, string arguments, CancellationToken token = default);
    }
}
