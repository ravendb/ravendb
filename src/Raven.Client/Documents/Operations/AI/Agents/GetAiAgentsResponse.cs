using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class GetAiAgentsResponse
{
    public List<AiAgentConfiguration> AiAgents { get; set; }
}
