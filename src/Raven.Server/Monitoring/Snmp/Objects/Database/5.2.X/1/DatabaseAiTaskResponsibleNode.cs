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

public sealed class DatabaseAiTaskResponsibleNode : DatabaseEtlScalarObjectBase<OctetString>
{
    public DatabaseAiTaskResponsibleNode(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int aiTaskIndex)
        : base(databaseName, etlName, landlord, databaseIndex, aiTaskIndex, SnmpOids.Databases.AiTasks.TaskResponsibleNode)
    {
    }

    protected override OctetString GetData(DocumentDatabase database)
    {
        var responsibleNode = GetResponsibleNode(database, EtlName);
        if (responsibleNode == null)
            return null;

        return new OctetString(responsibleNode);
    }

    private static string GetResponsibleNode(DocumentDatabase database, string aiTaskProcessName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name);
            if (rawRecord == null)
                return null;

            var topology = rawRecord.Topology;
            var rachisState = database.ServerStore.CurrentRachisState;

            var aiTaskConfig = FindAiTaskConfiguration(rawRecord, aiTaskProcessName);
            if (aiTaskConfig == null)
                return null;

            return topology.WhoseTaskIsIt(rachisState, aiTaskConfig);
        }
    }

    private static IDatabaseTask FindAiTaskConfiguration(RawDatabaseRecord rawRecord, string processName)
    {
        return FindInConfigurations(rawRecord.EmbeddingsGenerations, processName)
               ?? FindInConfigurations(rawRecord.GenAis, processName);
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
