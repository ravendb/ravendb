using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseCdcSinkTaskResponsibleNode : DatabaseCdcSinkScalarObjectBase<OctetString>
{
    public DatabaseCdcSinkTaskResponsibleNode(string databaseName, string cdcSinkName, DatabasesLandlord landlord, int databaseIndex, int cdcSinkIndex)
        : base(databaseName, cdcSinkName, landlord, databaseIndex, cdcSinkIndex, SnmpOids.Databases.CdcSinks.TaskResponsibleNode)
    {
    }

    protected override OctetString GetData(DocumentDatabase database)
    {
        var responsibleNode = GetResponsibleNode(database, CdcSinkName);
        if (responsibleNode == null)
            return null;

        return new OctetString(responsibleNode);
    }

    private static string GetResponsibleNode(DocumentDatabase database, string cdcSinkName)
    {
        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name);
            if (rawRecord == null)
                return null;

            var topology = rawRecord.Topology;
            var rachisState = database.ServerStore.CurrentRachisState;

            var cdcSinks = rawRecord.CdcSinks;
            if (cdcSinks == null)
                return null;

            foreach (var config in cdcSinks)
            {
                if (config.Name == cdcSinkName)
                    return topology.WhoseTaskIsIt(rachisState, config);
            }

            return null;
        }
    }
}
