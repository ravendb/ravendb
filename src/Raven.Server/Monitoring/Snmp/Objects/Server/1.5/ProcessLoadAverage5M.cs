using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ProcessLoadAverage5M() : ProcessLoadAverageBase(SnmpOids.Server.ProcessLoadAverage5M, 1)
    {
    }
}
