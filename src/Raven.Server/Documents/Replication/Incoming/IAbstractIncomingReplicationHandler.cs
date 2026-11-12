using System;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication.Incoming
{
    public interface IAbstractIncomingReplicationHandler : IDisposable
    {
        public bool IsDisposed { get; }
        public IncomingConnectionInfo ConnectionInfo { get; }
        public string SourceFormatted { get; }
        public long LastDocumentEtag { get; }
        public IncomingReplicationPerformanceStats[] GetReplicationPerformance();
        public IncomingReplicationStatsAggregator GetLatestReplicationPerformance();
        public LiveReplicationPerformanceCollector.ReplicationPerformanceType GetReplicationPerformanceType();
        public DynamicJsonValue GetConnectionInfoAsJson();
    }
}
