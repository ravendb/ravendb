using System.Collections.Generic;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp;

public sealed class UpdateSnmpDatabaseEtlsMappingCommand : UpdateSnmpDatabaseMappingCommand
{
    public static string GetStorageKey(string databaseName)
    {
        return $"{Helpers.ClusterStateMachineValuesPrefix(databaseName)}/monitoring/snmp/etls/mapping";
    }

    public List<string> Etls { get; set; }

    protected override List<string> Items => Etls;

    protected override string ItemsPropertyName => nameof(Etls);

    protected override string GetStorageKeyForDatabase(string databaseName) => GetStorageKey(databaseName);

    public UpdateSnmpDatabaseEtlsMappingCommand()
    {
        // for deserialization
    }

    public UpdateSnmpDatabaseEtlsMappingCommand(string databaseName, List<string> etls, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
        Etls = etls;
    }
}
