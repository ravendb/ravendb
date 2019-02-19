using System;

namespace Sparrow.Server.Meters
{
    public class DatabasePerformanceMetrics
    {
        public enum MetricType
        {
            Transaction,
            GeneralWait
        }

        private readonly PerformanceMetrics _buffer;

        public PerformanceMetrics Buffer => _buffer;

        public DatabasePerformanceMetrics(MetricType type, int currentBufferSize, int summaryBufferSize)
        {
            switch (type)
            {
                case MetricType.GeneralWait:
                    _buffer = new GeneralWaitPerformanceMetrics(currentBufferSize, summaryBufferSize);
                    break;
                case MetricType.Transaction:
                    _buffer = new TransactionPerformanceMetrics(currentBufferSize, summaryBufferSize);
                    break;
                default:
                    throw new ArgumentException("Invalid metric type passed to DatabasePerfomanceMetrics " + type);
            }
        }

        public PerformanceMetrics.DurationMeasurement MeterPerformanceRate()
        {
            return new PerformanceMetrics.DurationMeasurement(_buffer);
        }

    }
}