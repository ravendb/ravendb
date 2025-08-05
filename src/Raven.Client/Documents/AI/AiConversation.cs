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
    private readonly List<AiAgentActionResponse> _actionResponses = [];
    private string _userPrompt;
    private string _changeVector;
    public string ChangeVector => _changeVector;

    private delegate Task<object> HandleActionDelegate(JsonOperationContext context, string arguments, CancellationToken token);

    private readonly Dictionary<string, HandleActionDelegate> _invocations = new();

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

        using (_aiOperations.AllocateOperationContext(out var context))
        {
            var jsonSerializer = _aiOperations._store.Conventions.Serialization.DefaultConverter;
            var json = jsonSerializer.ToBlittable(actionResponse, context);
            AddActionResponse(toolId, json.ToString());
        }
    }

    public void AddActionResponse(string toolId, string actionResponse)
    {
        ValidationMethods.AssertNotNullOrEmpty(toolId, nameof(toolId));
        ValidationMethods.AssertNotNullOrEmpty(actionResponse, nameof(actionResponse));

        _actionResponses.Add(new AiAgentActionResponse
        {
            ToolId = toolId, 
            Content = actionResponse
        });
    }

    public void SetUserPrompt(string userPrompt)
    {
        ValidationMethods.AssertNotNullOrEmpty(userPrompt, nameof(userPrompt));

        _userPrompt = userPrompt;
    }

    public void Handle<TArgs>(string actionName, Func<TArgs, Task<object>> action, AiHandleErrorStrategy errorStrategy)
        where TArgs : class
    {
        var t = new AiActionContext<TArgs>(_aiOperations, action, errorStrategy);
        AddAction(actionName, t.ExecuteAsync);
    }

    public void Handle<TArgs>(string actionName, Func<TArgs, object> action, AiHandleErrorStrategy errorStrategy) where TArgs : class
    {
        var t = new AiActionContext<TArgs>(_aiOperations, action, errorStrategy);
        AddAction(actionName, t.ExecuteAsync);
    }

    private void AddAction(string actionName, HandleActionDelegate t)
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

            using (_aiOperations.AllocateOperationContext(out JsonOperationContext ctx))
            {
                foreach (var action in _actionRequests)
                {
                    if (_invocations.TryGetValue(action.Name, out var invocation))
                    {
                        // error handling here is expected to be done by the invocation based on the error strategy the user choose
                        var response = await invocation.Invoke(ctx, action.Arguments, token).ConfigureAwait(false);
                        AddActionResponse(action.ToolId, response);
                    }
                }
            }

            // We have nothing to tell the server, but still have action requests pending
            // we need to send those action requests to the caller that can handle them
            if (_actionResponses.Count == 0)
                return r; // note - this has ActionsRequired status
        }
    }

    private async Task<AiAnswer<TAnswer>> RunAsyncInternal<TAnswer>(CancellationToken token = default)
    {
        if (
            // if this is null, it is the first time we call RunAsync, so we are going to the server to get the pending actions
            _actionRequests != null &&
            // otherwise, we already went to the server and have nothing new to tell it, so we are done
            string.IsNullOrEmpty(_userPrompt) && _actionResponses.Count == 0)
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

    private class AiActionContext<TActionParametersSchema>
        where TActionParametersSchema : class
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
            catch (Exception e) when (_errorStrategy is AiHandleErrorStrategy.SendErrorsToModel)
            {
                return CreateErrorMessageForLlm(e);
            }
        }

        private static string CreateErrorMessageForLlm(Exception e)
        {
            //TODO: We create a string here that would include the exception type + message recursively
            // we need avoid sending stack trace to the model
            return e.Message;
        }
    }
}
