
using Metrics.Core;
using Metrics.Utils;
namespace Metrics
{
    public sealed class DefaultMetricsContext : BaseMetricsContext
    {
        public DefaultMetricsContext()
            : this(string.Empty) { }

        public DefaultMetricsContext(string context, MetricsRegistry registry = null, MetricsBuilder builder = null)
            : base(context, registry, builder, () => Clock.UTCDateTime)
        { }

        protected override MetricsContext CreateChildContextInstance(string contextName)
        {
            return new DefaultMetricsContext(contextName,new DefaultMetricsRegistry(), new DefaultMetricsBuilder());
        }
    }
}
