using System;
using System.IO;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Voron.Util;

namespace Raven.Server.Documents.TcpHandlers
{
    public class TcpConnectionOptions : IDisposable
    {
        private static long _sequence;

        private MeterMetric _bytesReceivedMetric;
        private MeterMetric _bytesSentMetric;
        private readonly DateTime _connectedAt;
        public long _lastEtagSent;
        public long _lastEtagReceived;

        private bool _isDisposed;

        public DocumentDatabase DocumentDatabase;

        public TcpConnectionHeaderMessage.OperationTypes Operation;

        public Stream Stream;

        public TcpClient TcpClient;

        public int ProtocolVersion;

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
        public JsonContextPool ContextPool;

        private readonly SemaphoreSlim _running = new SemaphoreSlim(1);
        private string _debugTag;
        public X509Certificate2 Certificate;

        public override string ToString()
        {
            return "Tcp Connection " + _debugTag;
        }
        public IDisposable ConnectionProcessingInProgress(string debugTag)
        {
            _debugTag = debugTag;
            _running.Wait();
            return new DisposableAction(() => _running.Release());
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

#if !RELEASE
            GC.SuppressFinalize(this);
#endif

            Stream?.Dispose();
            TcpClient?.Dispose();

            _running.Wait();
            try
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;

                DocumentDatabase?.RunningTcpConnections.TryRemove(this);

                Stream = null;
                TcpClient = null;
                _bytesReceivedMetric = null;
                _bytesSentMetric = null;
            }
            finally
            {
                _running.Release();
            }
            // we'll let the _running be finalized, because otherwise we have
            // a possible race condition on dispose
        }

#if !RELEASE
        ~TcpConnectionOptions()
        {
            throw new LowMemoryException($"Detected a leak on TcpConnectionOptions ('{ToString()}') when running the finalizer.");
        }
#endif

        public void RegisterBytesSent(long bytesAmount)
        {
            _bytesSentMetric?.Mark(bytesAmount);
        }

        public void RegisterBytesReceived(long bytesAmount)
        {
            _bytesReceivedMetric?.Mark(bytesAmount);
        }

        public bool CheckMatch(long? minSecondsDuration, long? maxSecondsDuration, string ip,
            TcpConnectionHeaderMessage.OperationTypes? operationType)
        {
            var totalSeconds = (long)(DateTime.UtcNow - _connectedAt).TotalSeconds;

            if (totalSeconds < minSecondsDuration)
                return false;

            if (totalSeconds > maxSecondsDuration)
                return false;

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

        public DynamicJsonValue GetConnectionStats()
        {
            var stats = new DynamicJsonValue
            {
                ["Id"] = Id,
                ["Operation"] = Operation.ToString(),
                ["ClientUri"] = TcpClient?.Client?.RemoteEndPoint?.ToString(),
                ["ConnectedAt"] = _connectedAt,
                ["Duration"] = (DateTime.UtcNow - _connectedAt).ToString(),
                ["LastEtagReceived"] = _lastEtagReceived,
                ["LastEtagSent"] = _lastEtagSent
            };


            _bytesReceivedMetric?.SetMinimalHumaneMeterData("Received", stats);
            _bytesSentMetric?.SetMinimalHumaneMeterData("Sent", stats);


            _bytesReceivedMetric?.SetMinimalMeterData("Received", stats);
            _bytesSentMetric?.SetMinimalMeterData("Sent", stats);

            return stats;
        }
    }
}
