using System;
using System.IO;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Sparrow.Platform;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public abstract class MachineLoadAverageBase : ScalarObjectBase<Gauge32>
    {
        private readonly int _index; // 0-based index: 0 => 1m, 1 => 5m, 2 => 15m

        protected MachineLoadAverageBase(string oid, int index) : base(oid)
        {
            _index = index;
        }

        protected override Gauge32 GetData()
        {
            if (PlatformDetails.RunningOnLinux == false)
                return new Gauge32(-1);

            try
            {
                const string loadAvgPath = "/proc/loadavg";
                if (File.Exists(loadAvgPath) == false)
                    return new Gauge32(-1);

                var content = File.ReadAllText(loadAvgPath);
                if (string.IsNullOrWhiteSpace(content))
                    return new Gauge32(-1);

                // Expected format: "0.08 0.15 0.20 1/123 4567"
                var parts = content.Split([' ', '\t'], StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 3)
                    return new Gauge32(-1);

                var token = parts[_index];
                if (double.TryParse(token, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) == false)
                    return new Gauge32(-1);

                var normalized = value / Environment.ProcessorCount * 100;
                return new Gauge32((int)normalized);
            }
            catch (Exception)
            {
                return new Gauge32(-1);
            }
        }
    }
}
