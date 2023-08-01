using System;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineProcessorCount : ScalarObjectBase<Integer32>
    {
        public MachineProcessorCount()
            : base(SnmpOids.Server.MachineProcessorCount)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32(Environment.ProcessorCount);
        }
    }
}
