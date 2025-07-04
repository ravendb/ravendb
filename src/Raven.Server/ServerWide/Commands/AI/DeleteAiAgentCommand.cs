using System;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.AI;

public class DeleteAiAgentCommand : UpdateDatabaseCommand
{
    public string AgentName;

    public DeleteAiAgentCommand()
    {
        // for deserialization    
    }

    public DeleteAiAgentCommand(string database, string agentName, string uniqueRequestId) : base(database, uniqueRequestId)
    {
        AgentName = agentName ?? throw new ArgumentNullException(nameof(agentName));
    }
    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        record.AiAgents.Remove(AgentName);
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(AgentName)] = AgentName;
    }
}
