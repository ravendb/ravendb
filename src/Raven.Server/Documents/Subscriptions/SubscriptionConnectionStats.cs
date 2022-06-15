using System;
using Raven.Server.Utils.Metrics;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionMetrics : IDisposable
    {
        public SubscriptionConnectionMetrics()
        {
            DocsRate = new MeterMetric();
            BytesRate = new MeterMetric();
            AckRate = new MeterMetric();
        }
        
        public DateTime ConnectedAt;

        public DateTime LastMessageSentAt;

        public DateTime LastAckReceivedAt;

        internal MeterMetric DocsRate;
        public MeterMetric BytesRate;
        public MeterMetric AckRate;

        public void Dispose()
        {
            DocsRate = null;
            BytesRate = null;
            AckRate = null;
        }
    }
}
