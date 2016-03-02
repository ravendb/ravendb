using System;
using System.Collections.Generic;
using Metrics.MetricData;

namespace Metrics.Core
{
    public sealed class DefaultRegistryDataProvider : RegistryDataProvider
    {
        private readonly Func<IEnumerable<Meter>> meters;
        private readonly Func<IEnumerable<Histogram>> histograms;
        

        public DefaultRegistryDataProvider(
            Func<IEnumerable<Meter>> meters,
            Func<IEnumerable<Histogram>> histograms)
        {
            this.meters = meters;
            this.histograms = histograms; 
        }
        
        public IEnumerable<Meter> Meters { get { return this.meters(); } }
        public IEnumerable<Histogram> Histograms { get { return this.histograms(); } }
    }
}
