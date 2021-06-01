using System;
using System.IO;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
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

        private static int _numberOfActiveInstances = 0;

        public TcpConnectionOptions()
        {
            _bytesReceivedMetric = new MeterMetric();
            _bytesSentMetric = new MeterMetric();

            MetricsScheduler.Instance.StartTickingMetric(_bytesSentMetric);
            MetricsScheduler.Instance.StartTickingMetric(_bytesReceivedMetric);
            _connectedAt = DateTime.UtcNow;

            Id = Interlocked.Increment(ref _sequence);
            Interlocked.Increment(ref _numberOfActiveInstances);
        }

        public long Id { get; set; }
        public JsonContextPool ContextPool;

        private readonly SemaphoreSlim _running = new SemaphoreSlim(1);
        private string _debugTag;
        public X509Certificate2 Certificate;

        public StringBuilder DebugInfo = new StringBuilder();

        public override string ToString()
        {
            return $"TCP Connection ('{Operation}') {_debugTag} - {DebugInfo}";
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

            Interlocked.Decrement(ref _numberOfActiveInstances);

            GC.SuppressFinalize(this);

            using (TcpClient)
            using (Stream)
            {
                if (Operation == TcpConnectionHeaderMessage.OperationTypes.Cluster)
                {
                    try
                    {
                        TcpClient?.Client?.Disconnect(reuseSocket: false);
                    }
                    catch
                    {
                        // nothing we can do
                    }
                }
            }

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
        
        ~TcpConnectionOptions()
        {
            string message = $"Detected a leak on TcpConnectionOptions ('{ToString()}') when running the finalizer. Number of active instances: {_numberOfActiveInstances} ({_sequence} created in total)";

            Console.WriteLine(message);

            throw new LowMemoryException(message);
        }

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
