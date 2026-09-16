using System.Collections.Generic;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp;

public sealed class UpdateSnmpDatabaseCdcSinksMappingCommand : UpdateSnmpDatabaseMappingCommand
{
    public static string GetStorageKey(string databaseName)
    {
        return $"{Helpers.ClusterStateMachineValuesPrefix(databaseName)}/monitoring/snmp/cdc-sinks/mapping";
    }

    public List<string> CdcSinks { get; set; }

    protected override List<string> Items => CdcSinks;

    protected override string ItemsPropertyName => nameof(CdcSinks);

    protected override string GetStorageKeyForDatabase(string databaseName) => GetStorageKey(databaseName);

    public UpdateSnmpDatabaseCdcSinksMappingCommand()
    {
        // for deserialization
    }

    public UpdateSnmpDatabaseCdcSinksMappingCommand(string databaseName, List<string> cdcSinks, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
        CdcSinks = cdcSinks;
    }
}
