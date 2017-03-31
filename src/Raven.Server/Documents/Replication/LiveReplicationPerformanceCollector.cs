using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class LiveReplicationPerformanceCollector : IDisposable
    {
        private readonly DocumentDatabase _database;
        private CancellationTokenSource _cts;

        private static readonly Sparrow.Collections.LockFree.ConcurrentDictionary<string, IncomingHandlerAndPerformanceStatsList> _incoming = new Sparrow.Collections.LockFree.ConcurrentDictionary<string, IncomingHandlerAndPerformanceStatsList>(StringComparer.OrdinalIgnoreCase);

        public LiveReplicationPerformanceCollector(DocumentDatabase database)
        {
            _database = database;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);

            Task.Factory.StartNew(StartCollectingStats);
        }

        public AsyncQueue<List<IReplicationPerformanceStats>> Stats { get; } = new AsyncQueue<List<IReplicationPerformanceStats>>();

        private async Task StartCollectingStats()
        {
            _database.ReplicationLoader.IncomingReplicationAdded += IncomingHandlerAdded;
            _database.ReplicationLoader.IncomingReplicationRemoved += IncomingHandlerRemoved;

            var token = _cts.Token;

            try
            {
                while (token.IsCancellationRequested == false)
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), token);

                    if (token.IsCancellationRequested)
                        break;

                    var performanceStats = PreparePerformanceStats().ToList();

                    if (performanceStats.Count > 0)
                    {
                        Stats.Enqueue(performanceStats);
                    }
                }
            }
            finally
            {
                _database.ReplicationLoader.IncomingReplicationRemoved -= IncomingHandlerRemoved;
                _database.ReplicationLoader.IncomingReplicationAdded -= IncomingHandlerAdded;
            }
        }

        private static IEnumerable<IReplicationPerformanceStats> PreparePerformanceStats()
        {
            foreach (var indexAndPerformanceStatsList in _incoming.Values)
            {
                var handler = indexAndPerformanceStatsList.Handler;
                var performance = indexAndPerformanceStatsList.Performance;

                var itemsToSend = new List<IncomingReplicationStatsAggregator>(performance.Count);

                IncomingReplicationStatsAggregator stat;
                while (performance.TryTake(out stat))
                    itemsToSend.Add(stat);

                var latestStats = handler.GetLatestReplicationPerformance();

                if (latestStats.Completed == false && itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                    yield return new IncomingPerformanceStats(handler.ConnectionInfo.SourceDatabaseId, handler.Source, itemsToSend.Select(item => item.ToReplicationPerformanceLiveStatsWithDetails()).ToArray());
            }
        }

        private void IncomingHandlerRemoved(string id)
        {
            IncomingHandlerAndPerformanceStatsList handler;
            if (_incoming.TryRemove(id, out handler))
                handler.Handler.DocumentsReceived -= IncomingDocumentsReceived;
        }

        private void IncomingHandlerAdded(string id, IncomingReplicationHandler handler)
        {
            _incoming.GetOrAdd(id, key =>
            {
                handler.DocumentsReceived += IncomingDocumentsReceived;

                return new IncomingHandlerAndPerformanceStatsList(handler);
            });
        }

        private void IncomingDocumentsReceived(IncomingReplicationHandler handler)
        {
            IncomingHandlerAndPerformanceStatsList stats;
            if (_incoming.TryGetValue(handler.ConnectionInfo.SourceDatabaseId, out stats) == false)
            {
                // possible?
                return;
            }

            var latestStat = stats.Handler.GetLatestReplicationPerformance();
            if (latestStat != null)
                stats.Performance.Add(latestStat, _cts.Token);
        }

        public void Dispose()
        {
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;
        }

        private class IncomingHandlerAndPerformanceStatsList
        {
            public readonly IncomingReplicationHandler Handler;

            public readonly BlockingCollection<IncomingReplicationStatsAggregator> Performance;

            public IncomingHandlerAndPerformanceStatsList(IncomingReplicationHandler handler)
            {
                Handler = handler;
                Performance = new BlockingCollection<IncomingReplicationStatsAggregator>();
            }
        }

        private class IncomingPerformanceStats : ReplicationPerformanceStatsBase<IncomingReplicationPerformanceStats>
        {
            public IncomingPerformanceStats(string id, string description, IncomingReplicationPerformanceStats[] performance)
                : base(id, description, ReplicationType.Incoming, performance)
            {
            }
        }

        public abstract class ReplicationPerformanceStatsBase<TPerformance> : IReplicationPerformanceStats
            where TPerformance : class
        {
            protected ReplicationPerformanceStatsBase(string id, string description, ReplicationType type, TPerformance[] performance)
            {
                Id = id;
                Description = description;
                Type = type;
                Performance = performance;
            }

            protected ReplicationType Type { get; }

            protected string Id { get; }

            protected string Description { get; }

            protected TPerformance[] Performance { get; }

            public void Write(JsonOperationContext context, BlittableJsonTextWriter writer)
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(Id));
                writer.WriteString(Id);
                writer.WriteComma();

                writer.WritePropertyName(nameof(Description));
                writer.WriteString(Description);
                writer.WriteComma();

                writer.WritePropertyName(nameof(Type));
                writer.WriteString(Type.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(Performance));
                writer.WriteArray(context, Performance, (w, c, p) =>
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(p);
                    w.WriteObject(c.ReadObject(djv, "incoming/replication/performance"));
                });

                writer.WriteEndObject();
            }

            public enum ReplicationType
            {
                Incoming,
                Outgoing
            }
        }

        public interface IReplicationPerformanceStats
        {
            void Write(JsonOperationContext context, BlittableJsonTextWriter writer);
        }
    }
}
