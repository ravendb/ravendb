using System.Collections.Generic;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseEtlTaskResponsibleNode : DatabaseEtlScalarObjectBase<OctetString>
{
    public DatabaseEtlTaskResponsibleNode(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.Etls.TaskResponsibleNode)
    {
    }

    protected override OctetString GetData(DocumentDatabase database)
    {
        var responsibleNode = GetResponsibleNode(database, EtlName);
        if (responsibleNode == null)
            return null;

        return new OctetString(responsibleNode);
    }

    private static string GetResponsibleNode(DocumentDatabase database, string etlProcessName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name);
            if (rawRecord == null)
                return null;

            var topology = rawRecord.Topology;
            var rachisState = database.ServerStore.CurrentRachisState;

            var etlConfig = FindEtlConfiguration(rawRecord, etlProcessName);
            if (etlConfig == null)
                return null;

            return topology.WhoseTaskIsIt(rachisState, etlConfig);
        }
    }

    private static IDatabaseTask FindEtlConfiguration(RawDatabaseRecord rawRecord, string processName)
    {
        return FindInConfigurations(rawRecord.RavenEtls, processName)
               ?? FindInConfigurations(rawRecord.SqlEtls, processName)
               ?? FindInConfigurations(rawRecord.OlapEtls, processName)
               ?? FindInConfigurations(rawRecord.ElasticSearchEtls, processName)
               ?? FindInConfigurations(rawRecord.QueueEtls, processName)
               ?? FindInConfigurations(rawRecord.SnowflakeEtls, processName);
    }

    private static IDatabaseTask FindInConfigurations<T>(IEnumerable<EtlConfiguration<T>> configurations, string processName) where T : ConnectionString
    {
        if (configurations == null)
            return null;

        foreach (var config in configurations)
        {
            foreach (var transform in config.Transforms)
            {
                if (EtlProcess.GetProcessName(config.Name, transform.Name) == processName)
                    return config;
            }
        }

        return null;
    }
}
