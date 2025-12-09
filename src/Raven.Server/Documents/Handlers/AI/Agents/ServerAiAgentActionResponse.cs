using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    public class ServerAiAgentActionResponse
    {
        public string Agent;
        public string ParentId;

        public List<AiAgentActionResponse> Responses;
    }
}
