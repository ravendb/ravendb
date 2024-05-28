using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineProcessorCount : ScalarObjectBase<Integer32>, IMetricInstrument<int>
    {
        public MachineProcessorCount()
            : base(SnmpOids.Server.MachineProcessorCount)
        {
        }

        private int Value => Environment.ProcessorCount;
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
