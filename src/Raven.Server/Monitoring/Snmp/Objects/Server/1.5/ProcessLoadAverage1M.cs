using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ProcessLoadAverage1M() : ProcessLoadAverageBase(SnmpOids.Server.ProcessLoadAverage1M, 0)
    {
    }
}
