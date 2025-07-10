using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Client.Documents.AI;

public interface IChatOperations<out T> where T : new()
{
    string Id { get; }
    T Answer { get; }
    AiUsage TotalUsage { get; }
    IEnumerable<ToolRequest> OpenTools();
    void AddToolResponse(string id, string toolResponse);
    void AddToolResponse(string id, object toolResponse);
    Task<IEnumerable<ChatMessage>> ReadMessagesAsync(CancellationToken token);
    Task<bool> RunAsync(CancellationToken token = default);
    bool Run();
    void SetUserPrompt(string userPrompt);
}
