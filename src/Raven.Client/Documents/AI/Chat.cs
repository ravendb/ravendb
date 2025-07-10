using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.AI;
internal class Chat<T> : IChatOperations<T> where T : new()
{
    private readonly AiOperations _aiOperations;
    private readonly string _agentId;
    private readonly Dictionary<string, object> _parameters;

    private string _chatId;
    private AiUsage _totalUsage;
    private List<ToolRequest> _toolsRequests;
    private List<ToolResponse> _toolsResponses = [];
    private string _userPrompt;

    private bool _firstRun = true;

    public Chat(AiOperations aiOperations, string agentId, Dictionary<string, object> parameters)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));

        _aiOperations = aiOperations;
        _agentId = agentId;
        _parameters = parameters;
    }

    public Chat(AiOperations aiOperations, string chatId)
    {
        ValidationMethods.AssertNotNullOrEmpty(chatId, nameof(chatId));

        _aiOperations = aiOperations;
        _chatId = chatId;
    }

    public IEnumerable<ToolRequest> OpenTools() => _toolsRequests ?? throw new InvalidOperationException($"You have to call {nameof(Run)}/{nameof(RunAsync)} first");

    public void AddToolResponse(string toolId, string toolResponse)
    {
        _toolsResponses.Add(new ToolResponse
        {
            ToolId = toolId,
            Content = toolResponse
        });
    }

    public void AddToolResponse(string toolId, object toolResponse)
    {
        if (toolResponse is string str)
        {
            AddToolResponse(toolId, str);
            return;
        }

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            var jsonSerializer = DocumentConventions.Default.Serialization.DefaultConverter;
            var json = jsonSerializer.ToBlittable(toolResponse, context);
            AddToolResponse(toolId, json.ToString());
        }
    }

    public void SetUserPrompt(string userPrompt)
    {
        ValidationMethods.AssertNotNullOrEmpty(userPrompt, nameof(userPrompt));

        _userPrompt = userPrompt;
    }

    private T _answer;
    public T Answer => _answer ?? throw new InvalidOperationException($"You have to call {nameof(Run)}/{nameof(RunAsync)} first");
    public string Id => _chatId ?? throw new InvalidOperationException($"This is a new chat, the ID wasn't set yet, you have to call {nameof(Run)}/{nameof(RunAsync)}");
    public AiUsage TotalUsage => _totalUsage ?? throw new InvalidOperationException($"You have to call {nameof(Run)}/{nameof(RunAsync)} first");

    public async Task<IEnumerable<ChatMessage>> ReadMessagesAsync(CancellationToken token)
    {
        var id = _chatId ?? throw new InvalidOperationException($"This is a new chat, the ID wasn't set yet, you have to call {nameof(Run)}/{nameof(RunAsync)}.");
        using var session = _aiOperations._store.OpenAsyncSession();
        var d = await session.LoadAsync<ChatDocument>(id, token).ConfigureAwait(false);
        return d.Messages;
    }

    public bool Run() => AsyncHelpers.RunSync(() => RunAsync());

    public async Task<bool> RunAsync(CancellationToken token = default)
    {
        // clear to avoid reusing old chat answer
        _answer = default;

        IMaintenanceOperation<ChatResult<T>> op;
        if (string.IsNullOrWhiteSpace(_chatId))
        {
            op = new RunChatOperation<T>(_agentId, _userPrompt, _parameters);
        }
        else
        {
            // we allow to run the chat only if it is the first run with no user prompt or tool requests
            // this way we can fetch the initial chat state
            if (_firstRun == false && string.IsNullOrEmpty(_userPrompt) && _toolsResponses.Count == 0)
                return false;

            op = new RunChatOperation<T>(_chatId, _userPrompt, _toolsResponses);
        }

        try
        {
            var r = await _aiOperations._executor.SendAsync(op, token).ConfigureAwait(false);
            _chatId = r.ChatId;
            _toolsRequests = r.ToolRequests ?? new List<ToolRequest>();
            _answer = r.Response;
            _totalUsage = r.Usage;
            _firstRun = false;
        }
        finally
        {
            // clear the user prompt and tool responses after running the chat
            _userPrompt = null;
            _toolsResponses.Clear();
        }

        return _toolsRequests.Count > 0;
    }
}

public class ChatDocument
{
    public List<ChatMessage> Messages;
}

public class ChatMessage
{
    public string role;
    public string content;
    public List<ChatToolCall> tool_calls;
    public string tool_call_id;
    public string refusal;
    public List<string> annotations;
}

public class ChatToolCall
{
    public string id;
    public string type;
    public ChatToolCallFunction function;
}

public class ChatToolCallFunction
{
    public string name;
    public string arguments;
}
