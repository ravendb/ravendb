using System;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseActiveCdcSinks : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseActiveCdcSinks(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.NumberOfActiveCdcSinks, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        var oneMinuteAgo = DateTime.UtcNow.AddMinutes(-1);
        return new Integer32(database.CdcSinkLoader.Processes.Count(x => x.GetLatestPerformanceStats()?.StartTime > oneMinuteAgo));
    }
}
