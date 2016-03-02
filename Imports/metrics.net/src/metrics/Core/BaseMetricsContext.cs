using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Metrics.MetricData;
using Metrics.Sampling;

namespace Metrics.Core
{
    public abstract class BaseMetricsContext : MetricsContext, AdvancedMetricsContext
    {
        private readonly ConcurrentDictionary<string, MetricsContext> childContexts = new ConcurrentDictionary<string, MetricsContext>();

        private MetricsRegistry registry;
        private MetricsBuilder metricsBuilder;

        private bool isDisabled;

        protected BaseMetricsContext(string context, MetricsRegistry registry, MetricsBuilder metricsBuilder, Func<DateTime> timestampProvider)
        {
            this.registry = registry;
            this.metricsBuilder = metricsBuilder;
        }

        protected abstract MetricsContext CreateChildContextInstance(string contextName);

        public AdvancedMetricsContext Advanced { get { return this; } }

        public event EventHandler ContextShuttingDown;
        public event EventHandler ContextDisabled;

        public MetricsContext Context(string contextName)
        {
            return this.Context(contextName, c => CreateChildContextInstance(contextName));
        }

        public MetricsContext Context(string contextName, Func<string, MetricsContext> contextCreator)
        {
            if (this.isDisabled)
            {
                return this;
            }

            if (string.IsNullOrEmpty(contextName))
            {
                return this;
            }

            return this.childContexts.GetOrAdd(contextName, contextCreator);
        }

        public void ShutdownContext(string contextName)
        {
            if (string.IsNullOrEmpty(contextName))
            {
                throw new ArgumentException("contextName must not be null or empty", contextName);
            }

            MetricsContext context;
            if (this.childContexts.TryRemove(contextName, out context))
            {
                using (context) { }
            }
        }

        public Meter Meter(string name)
        {
            return this.registry.Meter(name, x => this.metricsBuilder.BuildMeter(name));
        }

        public Histogram Histogram(string name)
        {
            return this.registry.Histogram(name, x => this.metricsBuilder.BuildHistogram(name));
        }

        public Meter PerSecondMetric(string name)
        {
            return this.registry.PerSecondMetric(name, x => this.metricsBuilder.BuildPerSecondMeter(name));
        }

        public Meter BufferedAverageMeter(string name, int bufferSize = 10, int intervalInSeconds = 1)
        {
            return this.registry.BufferedAverageMeter(name, x=> this.metricsBuilder.BuildBufferenAverageMeter(name, bufferSize,intervalInSeconds));
        }

        public void CompletelyDisableMetrics()
        {
            if (this.isDisabled)
            {
                return;
            }

            this.isDisabled = true;

            var oldRegistry = this.registry;
            this.registry = new NullMetricsRegistry();
            oldRegistry.ClearAllMetrics();
            using (oldRegistry as IDisposable) { }

            ForAllChildContexts(c => c.Advanced.CompletelyDisableMetrics());

            if (this.ContextShuttingDown != null)
            {
                this.ContextShuttingDown(this, EventArgs.Empty);
            }

            if (this.ContextDisabled != null)
            {
                this.ContextDisabled(this, EventArgs.Empty);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!this.isDisabled)
                {
                    if (this.ContextShuttingDown != null)
                    {
                        this.ContextShuttingDown(this, EventArgs.Empty);
                    }
                }
            }
        }

        public void ResetMetricsValues()
        {
            this.registry.ResetMetricsValues();
            ForAllChildContexts(c => c.Advanced.ResetMetricsValues());
        }

        private void ForAllChildContexts(Action<MetricsContext> action)
        {
            foreach (var context in this.childContexts.Values)
            {
                action(context);
            }
        }

    }
}
