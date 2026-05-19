using System.Collections.Generic;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp;

public sealed class UpdateSnmpDatabaseIndexesMappingCommand : UpdateSnmpDatabaseMappingCommand
{
    public static string GetStorageKey(string databaseName)
    {
        return $"{Helpers.ClusterStateMachineValuesPrefix(databaseName)}/monitoring/snmp/indexes/mapping";
    }

    public List<string> Indexes { get; set; }

    protected override List<string> Items => Indexes;

    protected override string ItemsPropertyName => nameof(Indexes);

    protected override string GetStorageKeyForDatabase(string databaseName) => GetStorageKey(databaseName);

    public UpdateSnmpDatabaseIndexesMappingCommand()
    {
        // for deserialization
    }

    public UpdateSnmpDatabaseIndexesMappingCommand(string databaseName, List<string> indexes, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
        Indexes = indexes;
    }
}
