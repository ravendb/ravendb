using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineLoadAverage15M() : MachineLoadAverageBase(SnmpOids.Server.MachineLoadAverage15M, 2)
    {
    }
}
