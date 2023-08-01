using System.Reflection;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Server.Platform.Posix;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public static class ServerMemInfo
    {
        public static void Register(ObjectStore store, MetricCacher metricCacher)
        {
            foreach (var propertyInfo in MemInfo.AllProperties.Values)
                store.Add(new ServerMemInfoValue(propertyInfo, metricCacher));
        }

        private sealed class ServerMemInfoValue : ScalarObjectBase<Gauge32>
        {
            private readonly PropertyInfo _propertyInfo;
            private readonly MetricCacher _metricCacher;

            public ServerMemInfoValue(PropertyInfo propertyInfo, MetricCacher metricCacher)
                : base(string.Format(SnmpOids.Server.MemInfoPrefix, propertyInfo.GetCustomAttribute<SnmpIndexAttribute>().Index))
            {
                _propertyInfo = propertyInfo;
                _metricCacher = metricCacher;
            }

            protected override Gauge32 GetData()
            {
                var memInfo = _metricCacher.GetValue<MemInfo>(MetricCacher.Keys.Server.MemInfo);
                var value = (Size)_propertyInfo.GetValue(memInfo);

                return new Gauge32(value.GetValue(SizeUnit.Bytes));
            }
        }
    }
}
