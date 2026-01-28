using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    public class SubAgentActionResponse
    {
        public string Agent;
        public string ParentId;

        public List<AiAgentActionResponse> Responses;
    }
}
