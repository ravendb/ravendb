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
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class LiveReplicationPerformanceCollector : IDisposable
    {
        private readonly DocumentDatabase _database;
        private CancellationTokenSource _cts;
        private readonly Task<Task> _task;

        private readonly ConcurrentDictionary<string, ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator>> _incoming =
            new ConcurrentDictionary<string, ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator>>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<OutgoingReplicationHandler, ReplicationHandlerAndPerformanceStatsList<OutgoingReplicationHandler, OutgoingReplicationStatsAggregator>> _outgoing =
            new ConcurrentDictionary<OutgoingReplicationHandler, ReplicationHandlerAndPerformanceStatsList<OutgoingReplicationHandler, OutgoingReplicationStatsAggregator>>();

        public LiveReplicationPerformanceCollector(DocumentDatabase database)
        {
            _database = database;

            var recentStats = PrepareInitialPerformanceStats().ToList();
            if (recentStats.Count > 0)
            {
                Stats.Enqueue(recentStats);
            }

            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            _task = Task.Factory.StartNew(StartCollectingStats);
        }

        public AsyncQueue<List<IReplicationPerformanceStats>> Stats { get; } = new AsyncQueue<List<IReplicationPerformanceStats>>();

        private async Task StartCollectingStats()
        {
            _database.ReplicationLoader.IncomingReplicationAdded += IncomingHandlerAdded;
            _database.ReplicationLoader.IncomingReplicationRemoved += IncomingHandlerRemoved;
            _database.ReplicationLoader.OutgoingReplicationAdded += OutgoingHandlerAdded;
            _database.ReplicationLoader.OutgoingReplicationRemoved += OutgoingHandlerRemoved;

            foreach (var handler in _database.ReplicationLoader.IncomingHandlers)
                IncomingHandlerAdded(handler.ConnectionInfo.SourceDatabaseId, handler);

            foreach (var handler in _database.ReplicationLoader.OutgoingHandlers)
                OutgoingHandlerAdded(handler);

            var token = _cts.Token;

            try
            {
                while (token.IsCancellationRequested == false)
                {
                    await TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(3000), token).ConfigureAwait(false);

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
                _database.ReplicationLoader.OutgoingReplicationRemoved -= OutgoingHandlerRemoved;
                _database.ReplicationLoader.OutgoingReplicationAdded -= OutgoingHandlerAdded;
                _database.ReplicationLoader.IncomingReplicationRemoved -= IncomingHandlerRemoved;
                _database.ReplicationLoader.IncomingReplicationAdded -= IncomingHandlerAdded;

                foreach (var kvp in _incoming)
                    IncomingHandlerRemoved(kvp.Key);

                foreach (var kvp in _outgoing)
                    OutgoingHandlerRemoved(kvp.Key);
            }
        }

        private IEnumerable<IReplicationPerformanceStats> PrepareInitialPerformanceStats()
        {
            foreach (var handler in _database.ReplicationLoader.IncomingHandlers)
            {
                var stats = handler.GetReplicationPerformance();
                if (stats.Length > 0)
                    yield return new IncomingPerformanceStats(handler.ConnectionInfo.SourceDatabaseId, handler.SourceFormatted, stats);
            }

            foreach (var handler in _database.ReplicationLoader.OutgoingHandlers)
            {
                var stats = handler.GetReplicationPerformance();
                if (stats.Length > 0)
                    yield return new OutgoingPerformanceStats(handler.DestinationDbId, handler.DestinationFormatted, stats);
            }
        }

        private IEnumerable<IReplicationPerformanceStats> PreparePerformanceStats()
        {
            foreach (var incoming in _incoming.Values)
            {
                var handler = incoming.Handler;
                var performance = incoming.Performance;

                var itemsToSend = new List<IncomingReplicationStatsAggregator>(performance.Count);

                IncomingReplicationStatsAggregator stat;
                while (performance.TryTake(out stat))
                    itemsToSend.Add(stat);

                var latestStats = handler.GetLatestReplicationPerformance();

                if (latestStats != null && latestStats.Completed == false && itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                    yield return new IncomingPerformanceStats(handler.ConnectionInfo.SourceDatabaseId, handler.SourceFormatted, itemsToSend.Select(item => item.ToReplicationPerformanceLiveStatsWithDetails()).ToArray());
            }

            foreach (var outgoing in _outgoing.Values)
            {
                var handler = outgoing.Handler;
                var performance = outgoing.Performance;

                var itemsToSend = new List<OutgoingReplicationStatsAggregator>(performance.Count);

                OutgoingReplicationStatsAggregator stat;
                while (performance.TryTake(out stat))
                    itemsToSend.Add(stat);

                var latestStats = handler.GetLatestReplicationPerformance();

                if (latestStats != null && latestStats.Completed == false && itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                    yield return new OutgoingPerformanceStats(handler.DestinationDbId, handler.DestinationFormatted, itemsToSend.Select(item => item.ToReplicationPerformanceLiveStatsWithDetails()).ToArray());
            }
        }

        private void OutgoingHandlerRemoved(OutgoingReplicationHandler handler)
        {
            ReplicationHandlerAndPerformanceStatsList<OutgoingReplicationHandler, OutgoingReplicationStatsAggregator> stats;
            if (_outgoing.TryRemove(handler, out stats))
                stats.Handler.DocumentsSend -= OutgoingDocumentsSend;
        }

        private void OutgoingHandlerAdded(OutgoingReplicationHandler handler)
        {
            _outgoing.GetOrAdd(handler, key =>
            {
                handler.DocumentsSend += OutgoingDocumentsSend;

                return new ReplicationHandlerAndPerformanceStatsList<OutgoingReplicationHandler, OutgoingReplicationStatsAggregator>(handler);
            });
        }

        private void OutgoingDocumentsSend(OutgoingReplicationHandler handler)
        {
            ReplicationHandlerAndPerformanceStatsList<OutgoingReplicationHandler, OutgoingReplicationStatsAggregator> stats;
            if (_outgoing.TryGetValue(handler, out stats) == false)
            {
                // possible?
                return;
            }

            var latestStat = stats.Handler.GetLatestReplicationPerformance();
            if (latestStat != null)
                stats.Performance.Add(latestStat, _cts.Token);
        }

        private void IncomingHandlerRemoved(string id)
        {
            ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator> stats;
            if (_incoming.TryRemove(id, out stats))
                stats.Handler.DocumentsReceived -= IncomingDocumentsReceived;
        }

        private void IncomingHandlerAdded(string id, IncomingReplicationHandler handler)
        {
            _incoming.GetOrAdd(id, key =>
            {
                handler.DocumentsReceived += IncomingDocumentsReceived;

                return new ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator>(handler);
            });
        }

        private void IncomingDocumentsReceived(IncomingReplicationHandler handler)
        {
            ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator> stats;
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

            try
            {
                _task.Wait();
            }
            catch (OperationCanceledException)
            {
            }

            _cts?.Dispose();
            _cts = null;
        }

        private class ReplicationHandlerAndPerformanceStatsList<THandler, TStatsAggregator>
        {
            public readonly THandler Handler;

            public readonly BlockingCollection<TStatsAggregator> Performance;

            public ReplicationHandlerAndPerformanceStatsList(THandler handler)
            {
                Handler = handler;
                Performance = new BlockingCollection<TStatsAggregator>();
            }
        }

        public class OutgoingPerformanceStats : ReplicationPerformanceStatsBase<OutgoingReplicationPerformanceStats>
        {
            public OutgoingPerformanceStats(string id, string description, OutgoingReplicationPerformanceStats[] performance)
                : base(id, description, ReplicationPerformanceType.Outgoing, performance)
            {
            }
        }

        public class IncomingPerformanceStats : ReplicationPerformanceStatsBase<IncomingReplicationPerformanceStats>
        {
            public IncomingPerformanceStats(string id, string description, IncomingReplicationPerformanceStats[] performance)
                : base(id, description, ReplicationPerformanceType.Incoming, performance)
            {
            }
        }

        public abstract class ReplicationPerformanceStatsBase<TPerformance> : IReplicationPerformanceStats
            where TPerformance : ReplicationPerformanceBase
        {
            protected ReplicationPerformanceStatsBase(string id, string description, ReplicationPerformanceType type, TPerformance[] performance)
            {
                Id = id;
                Description = description;
                Type = type;
                Performance = performance;
            }

            public ReplicationPerformanceType Type { get; }

            public string Id { get; }

            public string Description { get; }

            public TPerformance[] Performance { get; }

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

                writer.WriteArray(context, nameof(Performance), Performance, (w, c, p) =>
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(p);
                    w.WriteObject(c.ReadObject(djv, "incoming/replication/performance"));
                });

                writer.WriteEndObject();
            }
        }

        public enum ReplicationPerformanceType
        {
            Incoming,
            Outgoing
        }

        public interface IReplicationPerformanceStats
        {
            void Write(JsonOperationContext context, BlittableJsonTextWriter writer);
        }
    }
}
