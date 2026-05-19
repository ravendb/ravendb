using System.Collections.Generic;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp;

public sealed class UpdateSnmpDatabaseAiTasksMappingCommand : UpdateSnmpDatabaseMappingCommand
{
    public static string GetStorageKey(string databaseName)
    {
        return $"{Helpers.ClusterStateMachineValuesPrefix(databaseName)}/monitoring/snmp/ai-tasks/mapping";
    }

    public List<string> AiTasks { get; set; }

    protected override List<string> Items => AiTasks;

    protected override string ItemsPropertyName => nameof(AiTasks);

    protected override string GetStorageKeyForDatabase(string databaseName) => GetStorageKey(databaseName);

    public UpdateSnmpDatabaseAiTasksMappingCommand()
    {
        // for deserialization
    }

    public UpdateSnmpDatabaseAiTasksMappingCommand(string databaseName, List<string> aiTasks, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
        AiTasks = aiTasks;
    }
}
