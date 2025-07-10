using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Client.Documents.AI;

public interface IConversationOperations<out T> where T : new()
{
    string Id { get; }
    T Answer { get; }
    IEnumerable<AiAgentActionRequest> RequiredActions();
    void AddActionResponse(string actionId, string actionResponse);
    void AddActionResponse<TResponse>(string actionId, TResponse actionResponse) where TResponse : class;
    Task<bool> RunAsync(CancellationToken token = default);
    bool Run();
    void SetUserPrompt(string userPrompt);
}
