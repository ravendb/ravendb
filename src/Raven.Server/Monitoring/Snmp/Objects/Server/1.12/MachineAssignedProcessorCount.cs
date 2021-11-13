using System.Diagnostics;
using Lextm.SharpSnmpLib;
using Sparrow.Binary;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class MachineAssignedProcessorCount : ScalarObjectBase<Integer32>
    {
        public MachineAssignedProcessorCount()
            : base(SnmpOids.Server.MachineAssignedProcessorCount)
        {
        }

        protected override Integer32 GetData()
        {
            using (var currentProcess = Process.GetCurrentProcess())
#pragma warning disable CA1416 // Validate platform compatibility
                return new Integer32((int)Bits.NumberOfSetBits(currentProcess.ProcessorAffinity.ToInt64()));
#pragma warning restore CA1416 // Validate platform compatibility
        }
    }
}
