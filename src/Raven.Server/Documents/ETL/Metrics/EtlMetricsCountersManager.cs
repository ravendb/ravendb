using System;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Metrics
{
    public class EtlMetricsCountersManager
    {
        private readonly double[] _processedPerSecondRate = new double[8];
        private int _index;

        public EtlMetricsCountersManager()
        {
            BatchSizeMeter = new MeterMetric();
        }

        public MeterMetric BatchSizeMeter { get; protected set; }

        public void UpdateProcessedPerSecondRate(long batchSize, TimeSpan duration)
        {
            if (_index >= _processedPerSecondRate.Length)
                _index = 0;

            _processedPerSecondRate[_index++] = batchSize / duration.TotalSeconds;
        }

        public double? GetProcessedPerSecondRate()
        {
            var sum = 0.0;
            var count = 0;

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _processedPerSecondRate.Length; i++)
            {
                var value = _processedPerSecondRate[i];

                if (value > 0)
                {
                    sum += value;
                    count++;
                }
            }


            return count > 0 ? sum / count : (double?)null;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BatchSizeMeter)] = BatchSizeMeter.CreateMeterData()
            };
        }
    }
}
