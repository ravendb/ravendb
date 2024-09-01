using System.Diagnostics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow.Binary;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineAssignedProcessorCount : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        public MachineAssignedProcessorCount()
            : base(SnmpOids.Server.MachineAssignedProcessorCount)
        {
        }

        private int Value
        {
            get
            {
                using (var currentProcess = Process.GetCurrentProcess())
#pragma warning disable CA1416 // Validate platform compatibility
                    return (int)Bits.NumberOfSetBits(currentProcess.ProcessorAffinity.ToInt64());
#pragma warning restore CA1416 // Validate platform compatibility
            }
        }

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
