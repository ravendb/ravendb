using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineLoadAverage5M() : MachineLoadAverageBase(SnmpOids.Server.MachineLoadAverage5M, 1)
    {
    }
}
