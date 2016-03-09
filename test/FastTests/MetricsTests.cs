/*
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using metrics.Core;
using Xunit;

namespace FastTests
{
    public class MetricsTests
    {
        [Fact]
        public void Histogram()
        {
            var metrics = new metrics.Metrics();
            var histogram = metrics.Histogram("testMetrics", "simpleHistogram");

            var min = 0;
            var max = 10000;
            int sum = 0;
            var amount = max - min;
            double mean = 0;
            double stdev = 0;
            
            for (var i = min; i <= max; i++)
            {
                sum += i;
                histogram.Update(i);
            }

            mean = (double)sum / (amount);
            stdev = Math.Sqrt(Enumerable.Range(min, max).Sum(x => Math.Pow(x - mean, 2) / amount));

            Assert.Equal(10000, histogram.Max);
            Assert.Equal(0, histogram.Min);
            Assert.InRange(histogram.Mean, mean * 0.95, mean * 1.05);
            Assert.InRange(histogram.StdDev, stdev * 0.95, stdev * 1.05);

            
        }

        [Fact]
        public void MetricsTest()
        {
            var testMetrics = new metrics.Metrics();
            var perSecondMetrics = testMetrics.TimedCounter("metricsTest", "simpleCounter", "ping");

            for (var i = 0; i < 5; i++)
            {
                perSecondMetrics.Mark();
                SpinWait.SpinUntil(() => true, TimeSpan.FromMilliseconds(1000 ));
            }

            Console.WriteLine(perSecondMetrics.CurrentValue);
            


        }
    }
}
*/
