using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Util;

namespace Raven.Client.Documents.AI;
internal class Chat<T> : IChatOperations<T> where T : new()
{
    private readonly AiOperations _aiOperations;
    private readonly string _agent;
    private readonly Dictionary<string, object> _scope;

    private string _chatId;
    private AiUsage _totalUsage;
    private List<ToolRequest> _toolsRequests;
    private List<ToolResponse> _toolsResponses = [];
    private string _userPrompt;

    private bool _firstRun = true;
    public Chat(AiOperations aiOperations, string agent, Dictionary<string, object> scope)
    {
        _aiOperations = aiOperations;
        _agent = agent;
        _scope = scope;
    }
    public Chat(AiOperations aiOperations, string chatId)
    {
        _aiOperations = aiOperations;
        _chatId = chatId;
    }

    public IEnumerable<ToolRequest> OpenTools() => _toolsRequests ?? throw new InvalidOperationException("You have to call RunAsync first");

    public void AddToolResponse(string id, string content)
    {
        _toolsResponses.Add(new ToolResponse
        {
            ToolId = id,
            Content = content
        });
    }

    public bool Run() => AsyncHelpers.RunSync(() => RunAsync(CancellationToken.None));

    public void SetPrompt(string userPrompt)
    {
        _userPrompt = userPrompt;
    }

    private T _answer;
    public T Answer => _answer ?? throw new InvalidOperationException("You have to call RunAsync first");
    public string Id => _chatId ?? throw new InvalidOperationException("This is a new chat, the ID wasn't set yet, you have to call RunAsync");
    public AiUsage TotalUsage => _totalUsage ?? throw new InvalidOperationException("You have to call RunAsync first");

    public async Task<IEnumerable<ChatMessage>> ReadMessagesAsync(CancellationToken token)
    {
        var id = _chatId ?? throw new InvalidOperationException("This is a new chat, the ID wasn't set yet, you have to call RunAsync.");
        using var session = _aiOperations._store.OpenAsyncSession();
        var d = await session.LoadAsync<ChatDocument>(id, token).ConfigureAwait(false);
        return d.Messages;
    }

    public async Task<bool> RunAsync(CancellationToken token)
    {
        // clear to avoid reusing old chat answer
        _answer = default;

        IMaintenanceOperation<ChatResult<T>> op;
        if (string.IsNullOrWhiteSpace(_chatId))
        {
            op = new RunChatOperation<T>(_agent, _userPrompt, _scope);
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
