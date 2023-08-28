using System.Reflection;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Server.Platform.Posix;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public static class ServerLimits
    {
        public static void Register(ObjectStore store, MetricCacher metricCacher)
        {
            foreach (var propertyInfo in LimitsInfo.AllProperties.Values)
                store.Add(new ServerLimitsInfoValue(propertyInfo, metricCacher));
        }

        private class ServerLimitsInfoValue : ScalarObjectBase<Gauge32>
        {
            private readonly PropertyInfo _propertyInfo;
            private readonly MetricCacher _metricCacher;

            public ServerLimitsInfoValue(PropertyInfo propertyInfo, MetricCacher metricCacher)
                : base(string.Format(SnmpOids.Server.ServerLimitsPrefix, propertyInfo.GetCustomAttribute<SnmpIndexAttribute>().Index))
            {
                _propertyInfo = propertyInfo;
                _metricCacher = metricCacher;
            }

            protected override Gauge32 GetData()
            {
                var limitInfo = _metricCacher.GetValue<LimitsInfo>(MetricCacher.Keys.Server.CurrentServerLimits);
                var value = (long)_propertyInfo.GetValue(limitInfo);

                return new Gauge32(value);
            }
        }
    }
}
