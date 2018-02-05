using System;
using Raven.Server.Utils.Metrics;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionStats
    {
        public SubscriptionConnectionStats()
        {
            DocsRate = new MeterMetric();
            BytesRate = new MeterMetric();
            AckRate = new MeterMetric();
        }
        
        public DateTime ConnectedAt;

        public DateTime LastMessageSentAt;

        public DateTime LastAckReceivedAt;

        internal readonly MeterMetric DocsRate;
        public MeterMetric BytesRate;
        public MeterMetric AckRate;
    }
}
