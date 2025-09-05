using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineLoadAverage1M() : MachineLoadAverageBase(SnmpOids.Server.MachineLoadAverage1M, 0)
    {
    }
}
