using System;

namespace Raven.Server.Utils.Metrics.Core
{

    public sealed class FunctionGauge 
    {
        private readonly Func<double> valueProvider;

        public FunctionGauge(Func<double> valueProvider)
        {
            this.valueProvider = valueProvider;
        }

        public double GetValue(bool resetMetric = false)
        {
            return this.Value;
        }

        public double Value
        {
            get
            {
                try
                {
                    return this.valueProvider();
                }
                catch (Exception)
                {
                    return double.NaN;
                }
            }
        }
    }
}
