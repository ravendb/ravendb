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
        
        
        public TcpConnectionOptions()
        {
            
            _bytesReceivedMetric = new MeterMetric();
            _bytesSentMetric = new MeterMetric();

            MetricsScheduler.Instance.StartTickingMetric(_bytesSentMetric);
            MetricsScheduler.Instance.StartTickingMetric(_bytesReceivedMetric);
            _connectedAt = DateTime.UtcNow;

            Id = Interlocked.Increment(ref Sequence);
        }

        public long Id { get; set; }

        public TcpConnectionHeaderMessage.OperationTypes Operation;
        public DocumentDatabase DocumentDatabase;

        public JsonOperationContext Context;

        public NetworkStream Stream;

        public TcpClient TcpClient;

        public JsonOperationContext.MultiDocumentParser MultiDocumentParser;

        public List<IDisposable> DisposeOnConnectionClose = new List<IDisposable>();
        private MeterMetric _bytesReceivedMetric;
        private MeterMetric _bytesSentMetric;
        private DateTime _connectedAt;

        private bool _isDisposed;
        private static long Sequence = 0;
        public Action<BlittableJsonTextWriter, DocumentsOperationContext> GetTypeSpecificStats =
            (writer, context) => { writer.WriteNull();};


        public void RegisterBytesSent(long bytesAmount)
        {
            _bytesSentMetric.Mark(bytesAmount);
        }

        public void RegisterBytesReceived(long bytesAmount)
        {
            _bytesReceivedMetric.Mark(bytesAmount);
        }

        /*
         
              var duration = GetLongQueryString("duration");
            var ip = GetStringQueryString("ip");
            var type = GetStringQueryString("type");
            
             */

        public bool CheckMatch(long? minSecondsDuration, long? maxSecondsDuration, string ip,
            TcpConnectionHeaderMessage.OperationTypes? operationType)
        {
            var totalSeconds = (long)(DateTime.UtcNow - _connectedAt).TotalSeconds;

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
                if (operationType != this.Operation)
                    return false;
            }

            return true;
        }

        public void GetConnectionStats(BlittableJsonTextWriter writer, DocumentsOperationContext context)
        {
            context.Write(writer, 
            new DynamicJsonValue()
            {
                ["Id"] = Id,
                ["Operation"] = Operation.ToString(),
                ["ClientUri"] = TcpClient.Client.RemoteEndPoint.ToString(),
                ["ConnectedAt"] = _connectedAt,
                ["Duration"] = (DateTime.UtcNow - _connectedAt).ToString(),
                ["BytesReceived"] = _bytesReceivedMetric.CreateMeterData(),
                ["BytesSent"] = _bytesSentMetric.CreateMeterData()
            });
        }

        
        public void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;
            MetricsScheduler.Instance.StopTickingMetric(_bytesSentMetric);
            MetricsScheduler.Instance.StopTickingMetric(_bytesReceivedMetric);

            if (DocumentDatabase != null)
            {
                DocumentDatabase.RunningTcpConnections.TryRemove(this);
            }
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

        
        
    }
}