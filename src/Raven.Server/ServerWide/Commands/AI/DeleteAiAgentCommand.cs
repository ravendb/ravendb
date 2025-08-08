using System;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.AI;

public class DeleteAiAgentCommand : UpdateDatabaseCommand
{
    public string Identifier;

    public DeleteAiAgentCommand()
    {
        // for deserialization    
    }

    public DeleteAiAgentCommand(string database, string agentIdentifier, string uniqueRequestId) : base(database, uniqueRequestId)
    {
        Identifier = agentIdentifier ?? throw new ArgumentNullException(nameof(agentIdentifier));
    }
    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        record.AiAgents.RemoveAll(c => c.Identifier == Identifier);
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(Identifier)] = Identifier;
    }
}
