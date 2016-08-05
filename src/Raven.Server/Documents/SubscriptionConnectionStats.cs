using System;
using System.Net;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Server.Utils.Metrics;

namespace Raven.Server.Documents
{
    public class SubscriptionConnectionStats:IDisposable
    {
        public SubscriptionConnectionStats()
        {
            DocsRate = new MeterMetric();
            BytesRate = new MeterMetric();
            AckRate = new MeterMetric();
        }
        public long WaitedForConnection;
        public long ConnectedAt;

        public long LastMessageSentAt;
        
        public long LastSentEtag;

        public long LastAckReceivedAt;
        public long LastAckedEtag;

        internal readonly MeterMetric DocsRate;
        public MeterMetric BytesRate;
        public MeterMetric AckRate;

        public void Dispose()
        {
            DocsRate?.Dispose();
            BytesRate?.Dispose();
            AckRate?.Dispose();
        }
    }
}