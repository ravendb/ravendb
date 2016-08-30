using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.TcpHandlers
{
    public class TcpConnectionOptions : IDisposable
    {
        private static long _sequence;

        private readonly MeterMetric _bytesReceivedMetric;
        private readonly MeterMetric _bytesSentMetric;
        private readonly DateTime _connectedAt;

        private bool _isDisposed;

        public JsonOperationContext Context;

        public List<IDisposable> DisposeOnConnectionClose = new List<IDisposable>();
        public DocumentDatabase DocumentDatabase;

        public Action<JsonOperationContext, DynamicJsonValue> GetTypeSpecificStats;

        public JsonOperationContext.MultiDocumentParser MultiDocumentParser;

        public TcpConnectionHeaderMessage.OperationTypes Operation;

        public NetworkStream Stream;

        public TcpClient TcpClient;

        public TcpConnectionOptions()
        {
            _bytesReceivedMetric = new MeterMetric();
            _bytesSentMetric = new MeterMetric();

            MetricsScheduler.Instance.StartTickingMetric(_bytesSentMetric);
            MetricsScheduler.Instance.StartTickingMetric(_bytesReceivedMetric);
            _connectedAt = DateTime.UtcNow;

            Id = Interlocked.Increment(ref _sequence);
        }

        public long Id { get; set; }


        public void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            MetricsScheduler.Instance.StopTickingMetric(_bytesSentMetric);
            MetricsScheduler.Instance.StopTickingMetric(_bytesReceivedMetric);

            DocumentDatabase?.RunningTcpConnections.TryRemove(this);

            foreach (var disposable in DisposeOnConnectionClose)
            {
                try
                {
                    disposable?.Dispose();
                }
                catch (Exception)
                {
                    // nothing to do here
                }
            }
        }

        public void RegisterBytesSent(long bytesAmount)
        {
            _bytesSentMetric.Mark(bytesAmount);
        }

        public void RegisterBytesReceived(long bytesAmount)
        {
            _bytesReceivedMetric.Mark(bytesAmount);
        }

        public bool CheckMatch(long? minSecondsDuration, long? maxSecondsDuration, string ip,
            TcpConnectionHeaderMessage.OperationTypes? operationType)
        {
            var totalSeconds = (long) (DateTime.UtcNow - _connectedAt).TotalSeconds;

            if (minSecondsDuration.HasValue)
            {
                if (totalSeconds < minSecondsDuration.Value)
                    return false;
            }

            if (maxSecondsDuration.HasValue)
            {
                if (totalSeconds > maxSecondsDuration.Value)
                    return false;
            }

            if (string.IsNullOrEmpty(ip) == false)
            {
                if (TcpClient.Client.RemoteEndPoint.ToString().Equals(ip, StringComparison.OrdinalIgnoreCase) == false)
                    return false;
            }

            if (operationType.HasValue)
            {
                if (operationType != Operation)
                    return false;
            }

            return true;
        }

        public DynamicJsonValue GetConnectionStats(JsonOperationContext context)
        {
            var stats = new DynamicJsonValue
            {
                ["Id"] = Id,
                ["Operation"] = Operation.ToString(),
                ["ClientUri"] = TcpClient.Client.RemoteEndPoint.ToString(),
                ["ConnectedAt"] = _connectedAt,
                ["Duration"] = (DateTime.UtcNow - _connectedAt).ToString(),
                ["HumaneTotalReceived"] = Sizes.Humane(_bytesReceivedMetric.Count),
                ["HumaneReceivedRate"] = Sizes.Humane((long) _bytesReceivedMetric.OneMinuteRate),
                ["HumaneTotalSent"] = Sizes.Humane(_bytesSentMetric.Count),
                ["HumaneSentRate"] = Sizes.Humane((long) _bytesSentMetric.OneMinuteRate),

                ["TotalReceived"] = _bytesReceivedMetric.Count,
                ["ReceivedRate"] = Math.Round(_bytesReceivedMetric.OneMinuteRate, 2),
                ["TotalSent"] = _bytesSentMetric.Count,
                ["SentRate"] = Math.Round(_bytesSentMetric.OneMinuteRate, 2),
            };
            GetTypeSpecificStats?.Invoke(context, stats);
            return stats;
        }
    }
}