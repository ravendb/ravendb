using System;
using Lextm.SharpSnmpLib;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerTotalMemory : ScalarObjectBase<Gauge32>
    {
        private readonly Lazy<SmapsReader> _smapsReader;

        public ServerTotalMemory()
            : base(SnmpOids.Server.TotalMemory)
        {
            if (PlatformDetails.RunningOnLinux)
            {
                _smapsReader = new Lazy<SmapsReader>(() =>
                {
                    return new SmapsReader(new[] { new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize] });
                });
            }
        }

        protected override Gauge32 GetData()
        {
            var memoryInfo = MemoryInformation.GetMemoryInfo(_smapsReader?.Value, extended: true);

            return new Gauge32(memoryInfo.WorkingSet.GetValue(SizeUnit.Megabytes));
        }
    }
}
