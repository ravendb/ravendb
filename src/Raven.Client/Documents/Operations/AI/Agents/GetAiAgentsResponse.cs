using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// The server response containing one or more AI agent configurations.
/// </summary>
public class GetAiAgentsResponse
{
    /// <summary>
    /// The list of returned AI agent configurations. May contain zero, one, or many items.
    /// </summary>
    public List<AiAgentConfiguration> AiAgents { get; set; }
}
