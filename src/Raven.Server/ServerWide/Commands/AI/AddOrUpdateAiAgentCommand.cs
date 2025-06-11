using System;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.AiAgent;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.AI
{
    public class AddOrUpdateAiAgentCommand : UpdateDatabaseCommand
    {
        public string AgentName;
        public AiAgentConfiguration Configuration;

        public AddOrUpdateAiAgentCommand()
        {
            // for deserialization    
        }

        public AddOrUpdateAiAgentCommand(string database, string agentName, AiAgentConfiguration configuration, string uniqueRequestId) : base(database, uniqueRequestId)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration)); 
            AgentName = agentName ?? throw new ArgumentNullException(nameof(agentName));
        }
        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.AiAgents[AgentName] = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(AgentName)] = AgentName;
            json[nameof(Configuration)] = Configuration.ToJson();
        }
    }
}
