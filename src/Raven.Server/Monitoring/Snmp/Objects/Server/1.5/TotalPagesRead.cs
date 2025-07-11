using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Voron.Impl.Paging;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class TotalPagesRead() : ScalarObjectBase<Counter64>(SnmpOids.Server.TotalPagesRead), IMetricInstrument<long>
{
    protected override Counter64 GetData()
    {
        return new Counter64((ulong)PagingStatistics.GetTotals().TotalReads);
    }

    public long GetCurrentMeasurement() => PagingStatistics.GetTotals().TotalReads;
}
