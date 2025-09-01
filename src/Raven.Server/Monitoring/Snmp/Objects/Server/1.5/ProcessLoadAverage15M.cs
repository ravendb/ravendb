using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ProcessLoadAverage15M() : ProcessLoadAverageBase(SnmpOids.Server.ProcessLoadAverage15M, 2)
    {
    }
}
