
using System;
using Metrics.MetricData;
using Metrics.Sampling;

namespace Metrics.Core
{
    public interface MetricsBuilder
    {
        Meter BuildMeter(string name);
        Meter BuildPerSecondMeter(string name);
        Meter BuildBufferenAverageMeter(string name,int bufferSize=10,int intervalInSeconds=1);
        Histogram BuildHistogram(string name);

    }
}
