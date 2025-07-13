using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Util;

namespace Raven.Client.Documents.AI;
internal class Conversation<T> : IConversationOperations<T> where T : new()
{
    private readonly AiOperations _aiOperations;
    private readonly string _agentId;
    private readonly Dictionary<string, object> _parameters;

    private string _conversationId;
    private List<AiAgentActionRequest> _actionRequests;
    private List<AiAgentActionResponse> _actionResponses = [];
    private string _userPrompt;
    private string _changeVector;
    public Conversation(AiOperations aiOperations, string agentId, Dictionary<string, object> parameters)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));

        _aiOperations = aiOperations;
        _agentId = agentId;
        _parameters = parameters;
    }

    public Conversation(AiOperations aiOperations, string conversationId, string changeVector)
    {
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));

        _aiOperations = aiOperations;
        _conversationId = conversationId;
        _changeVector = changeVector;
    }

    public IEnumerable<AiAgentActionRequest> RequiredActions() => _actionRequests ?? throw new InvalidOperationException($"You have to call {nameof(Run)}/{nameof(RunAsync)} first");

    public void AddActionResponse<TResponse>(string actionId, TResponse actionResponse) where TResponse : class
    {
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

    private T _answer;
    public T Answer => _answer ?? throw new InvalidOperationException($"You have to call {nameof(Run)}/{nameof(RunAsync)} first");
    public string Id => _conversationId ?? throw new InvalidOperationException($"This is a new conversation, the ID wasn't set yet, you have to call {nameof(Run)}/{nameof(RunAsync)}");
    public bool Run() => AsyncHelpers.RunSync(() => RunAsync());

    public async Task<bool> RunAsync(CancellationToken token = default)
    {
        IMaintenanceOperation<ConversationResult<T>> op;
        if (string.IsNullOrWhiteSpace(_conversationId))
        {
            op = new RunConversationOperation<T>(_agentId, _userPrompt, _parameters);
        }
        else
        {
            // we allow to run the conversation only if it is the first run with no user prompt or tool requests
            // this way we can fetch the pending actions
            if (_actionRequests != null && string.IsNullOrEmpty(_userPrompt) && _actionResponses.Count == 0)
                return false;

            op = new RunConversationOperation<T>(_conversationId, _userPrompt, _actionResponses, _changeVector);
        }

        try
        {
            var r = await _aiOperations._executor.SendAsync(op, token).ConfigureAwait(false);
            r.ChangeVector = _changeVector;
            _conversationId = r.ConversationId;
            _actionRequests = r.ActionRequests ?? new List<AiAgentActionRequest>();
            _answer = r.Response;
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

        return _actionRequests.Count > 0;
    }
}
