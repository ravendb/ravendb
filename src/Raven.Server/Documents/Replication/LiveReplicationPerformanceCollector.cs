using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Server.Utils;
using Raven.Server.Utils.Stats;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class LiveReplicationPerformanceCollector : DatabaseAwareLivePerformanceCollector<LiveReplicationPerformanceCollector.IReplicationPerformanceStats>
    {
        private readonly ConcurrentDictionary<string, ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator>> _incoming =
            new ConcurrentDictionary<string, ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator>>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<OutgoingReplicationHandler, ReplicationHandlerAndPerformanceStatsList<OutgoingReplicationHandler, OutgoingReplicationStatsAggregator>> _outgoing =
            new ConcurrentDictionary<OutgoingReplicationHandler, ReplicationHandlerAndPerformanceStatsList<OutgoingReplicationHandler, OutgoingReplicationStatsAggregator>>();

        private readonly ConcurrentDictionary<ReplicationNode, OutgoingReplicationFailureToConnectReporter> _outgoingErrors = new ConcurrentDictionary<ReplicationNode, OutgoingReplicationFailureToConnectReporter>();
        private readonly ConcurrentDictionary<ReplicationNode, IncomingReplicationFailureToConnectReporter> _incomingErrors = new ConcurrentDictionary<ReplicationNode, IncomingReplicationFailureToConnectReporter>();

        public LiveReplicationPerformanceCollector(DocumentDatabase database) : base(database)
        {
            var recentStats = PrepareInitialPerformanceStats().ToList();
            if (recentStats.Count > 0)
            {
                Stats.Enqueue(recentStats);
            }

            Start();
        }

        protected override async Task StartCollectingStats()
        {
            Database.ReplicationLoader.IncomingReplicationAdded += IncomingHandlerAdded;
            Database.ReplicationLoader.IncomingReplicationRemoved += IncomingHandlerRemoved;
            Database.ReplicationLoader.OutgoingReplicationAdded += OutgoingHandlerAdded;
            Database.ReplicationLoader.OutgoingReplicationRemoved += OutgoingHandlerRemoved;
            Database.ReplicationLoader.OutgoingReplicationConnectionErrored += OutgoingReplicationConnectionFailed;
            Database.ReplicationLoader.IncomingReplicationConnectionErrored += IncomingReplicationConnectionFailed;

            foreach (var handler in Database.ReplicationLoader.IncomingHandlers)
                IncomingHandlerAdded(handler);

            foreach (var handler in Database.ReplicationLoader.OutgoingHandlers)
                OutgoingHandlerAdded(handler);

            try
            {
                await RunInLoop();
            }
            finally
            {
                Database.ReplicationLoader.OutgoingReplicationRemoved -= OutgoingHandlerRemoved;
                Database.ReplicationLoader.OutgoingReplicationAdded -= OutgoingHandlerAdded;
                Database.ReplicationLoader.IncomingReplicationRemoved -= IncomingHandlerRemoved;
                Database.ReplicationLoader.IncomingReplicationAdded -= IncomingHandlerAdded;
                Database.ReplicationLoader.OutgoingReplicationConnectionErrored -= OutgoingReplicationConnectionFailed;
                Database.ReplicationLoader.IncomingReplicationConnectionErrored -= IncomingReplicationConnectionFailed;

                foreach (var kvp in _incoming)
                    IncomingHandlerRemoved(kvp.Value.Handler);

                foreach (var kvp in _outgoing)
                    OutgoingHandlerRemoved(kvp.Key);
            }
        }

        private void OutgoingReplicationConnectionFailed(ReplicationNode node, OutgoingReplicationFailureToConnectReporter outgoingFailureReporter)
        {
            _outgoingErrors.AddOrUpdate(node, outgoingFailureReporter, (_, __) => outgoingFailureReporter);
        }

        private void IncomingReplicationConnectionFailed(ReplicationNode node, IncomingReplicationFailureToConnectReporter incomingFailureReporter)
        {
            _incomingErrors.AddOrUpdate(node, incomingFailureReporter, (_, __) => incomingFailureReporter);
        }

        protected IEnumerable<IReplicationPerformanceStats> PrepareInitialPerformanceStats()
        {
            foreach (var handler in Database.ReplicationLoader.IncomingHandlers)
            {
                var stats = handler.GetReplicationPerformance();
                if (stats.Length > 0)
                    yield return handler.PullReplication
                        ? IncomingPerformanceStats.ForPullReplication(handler.ConnectionInfo.SourceDatabaseId, handler.SourceFormatted, stats)
                        : IncomingPerformanceStats.ForPushReplication(handler.ConnectionInfo.SourceDatabaseId, handler.SourceFormatted, stats);
            }

            foreach (var handler in Database.ReplicationLoader.OutgoingHandlers)
            {
                var stats = handler.GetReplicationPerformance();
                if (stats.Length > 0)
                    yield return handler.IsPullReplicationAsHub
                        ? OutgoingPerformanceStats.ForPullReplication(handler.DestinationDbId, handler.DestinationFormatted, stats)
                        : OutgoingPerformanceStats.ForPushReplication(handler.DestinationDbId, handler.DestinationFormatted, stats);
            }
        }

        protected override List<IReplicationPerformanceStats> PreparePerformanceStats()
        {
            var results = new List<IReplicationPerformanceStats>(_incoming.Count + _outgoing.Count + _incomingErrors.Count + _outgoingErrors.Count);

            foreach (var incoming in _incoming)
            {
                // This is done this way instead of using
                // _incoming.Values because .Values locks the entire
                // dictionary.

                var handlerAndPerformanceStatsList = incoming.Value;
                var handler = handlerAndPerformanceStatsList.Handler;
                var performance = handlerAndPerformanceStatsList.Performance;

                var itemsToSend = new List<IncomingReplicationStatsAggregator>(performance.Count);
                while (performance.TryTake(out IncomingReplicationStatsAggregator stat))
                    itemsToSend.Add(stat);

                var latestStats = handler.GetLatestReplicationPerformance();

                if (latestStats != null && latestStats.Completed == false && itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                {
                    var stats = itemsToSend.Select(item => item.ToReplicationPerformanceLiveStatsWithDetails()).ToArray();
                    results.Add(handler.PullReplication
                        ? IncomingPerformanceStats.ForPullReplication(handler.ConnectionInfo.SourceDatabaseId, handler.SourceFormatted, stats)
                        : IncomingPerformanceStats.ForPushReplication(handler.ConnectionInfo.SourceDatabaseId, handler.SourceFormatted, stats));
                }
            }

            foreach (var outgoing in _outgoing)
            {
                // This is done this way instead of using
                // _outgoing.Values because .Values locks the entire
                // dictionary.

                var handlerAndPerformanceStatsList = outgoing.Value;
                var handler = handlerAndPerformanceStatsList.Handler;
                var performance = handlerAndPerformanceStatsList.Performance;

                var itemsToSend = new List<OutgoingReplicationStatsAggregator>(performance.Count);
                while (performance.TryTake(out OutgoingReplicationStatsAggregator stat))
                    itemsToSend.Add(stat);

                var latestStats = handler.GetLatestReplicationPerformance();

                if (latestStats != null && latestStats.Completed == false && itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                {
                    var stats = itemsToSend.Select(item => item.ToReplicationPerformanceLiveStatsWithDetails()).ToArray();
                    results.Add(handler.IsPullReplicationAsHub
                        ? OutgoingPerformanceStats.ForPullReplication(handler.DestinationDbId, handler.DestinationFormatted, stats)
                        : OutgoingPerformanceStats.ForPushReplication(handler.DestinationDbId, handler.DestinationFormatted, stats));
                }
            }

            foreach (var outgoingError in _outgoingErrors)
            {
                results.Add(OutgoingPerformanceStats.ForPushReplication(outgoingError.Key.Database, outgoingError.Value.DestinationFormatted, outgoingError.Value.GetReplicationPerformance()));
                _outgoingErrors.TryRemove(outgoingError.Key, out _);
            }

            foreach (var incomingError in _incomingErrors)
            {
                results.Add(IncomingPerformanceStats.ForPullReplication(incomingError.Key.Database, incomingError.Value.DestinationFormatted, incomingError.Value.GetReplicationPerformance()));
                _incomingErrors.TryRemove(incomingError.Key, out _);
            }

            return results;
        }

        protected override void WriteStats(List<IReplicationPerformanceStats> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", stats, (w, c, p) => p.Write(c, w));

            writer.WriteEndObject();
        }

        private void OutgoingHandlerRemoved(OutgoingReplicationHandler handler)
        {
            if (_outgoing.TryRemove(handler, out var stats))
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
            if (_outgoing.TryGetValue(handler, out var stats) == false)
            {
                // possible?
                return;
            }

            var latestStat = stats.Handler.GetLatestReplicationPerformance();
            if (latestStat != null)
                stats.Performance.Add(latestStat);
        }

        private void IncomingHandlerRemoved(IncomingReplicationHandler handler)
        {
            if (_incoming.TryRemove(handler.ConnectionInfo.SourceDatabaseId, out var stats))
                stats.Handler.DocumentsReceived -= IncomingDocumentsReceived;
        }

        private void IncomingHandlerAdded(IncomingReplicationHandler handler)
        {
            _incoming.GetOrAdd(handler.ConnectionInfo.SourceDatabaseId, key =>
            {
                handler.DocumentsReceived += IncomingDocumentsReceived;

                return new ReplicationHandlerAndPerformanceStatsList<IncomingReplicationHandler, IncomingReplicationStatsAggregator>(handler);
            });
        }

        private void IncomingDocumentsReceived(IncomingReplicationHandler handler)
        {
            if (_incoming.TryGetValue(handler.ConnectionInfo.SourceDatabaseId, out var stats) == false)
            {
                // possible?
                return;
            }

            var latestStat = stats.Handler.GetLatestReplicationPerformance();
            if (latestStat != null)
                stats.Performance.Add(latestStat);
        }

        private class ReplicationHandlerAndPerformanceStatsList<THandler, TStatsAggregator> : HandlerAndPerformanceStatsList<THandler, TStatsAggregator>
        {
            public ReplicationHandlerAndPerformanceStatsList(THandler handler) : base(handler)
            {
            }
        }

        public class OutgoingPerformanceStats : ReplicationPerformanceStatsBase<OutgoingReplicationPerformanceStats>
        {
            private OutgoingPerformanceStats(string id, string description, ReplicationPerformanceType type, OutgoingReplicationPerformanceStats[] performance)
                : base(id, description, type, performance)
            {
            }

            public static OutgoingPerformanceStats ForPullReplication(string id, string description, OutgoingReplicationPerformanceStats[] performance)
            {
                return new OutgoingPerformanceStats(id, description, ReplicationPerformanceType.OutgoingPull, performance);
            }

            public static OutgoingPerformanceStats ForPushReplication(string id, string description, OutgoingReplicationPerformanceStats[] performance)
            {
                return new OutgoingPerformanceStats(id, description, ReplicationPerformanceType.OutgoingPush, performance);
            }
        }

        public class IncomingPerformanceStats : ReplicationPerformanceStatsBase<IncomingReplicationPerformanceStats>
        {
            private IncomingPerformanceStats(string id, string description, ReplicationPerformanceType type, IncomingReplicationPerformanceStats[] performance)
                : base(id, description, type, performance)
            {
            }

            public static IncomingPerformanceStats ForPullReplication(string id, string description, IncomingReplicationPerformanceStats[] performance)
            {
                return new IncomingPerformanceStats(id, description, ReplicationPerformanceType.IncomingPull, performance);
            }

            public static IncomingPerformanceStats ForPushReplication(string id, string description, IncomingReplicationPerformanceStats[] performance)
            {
                return new IncomingPerformanceStats(id, description, ReplicationPerformanceType.IncomingPush, performance);
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

            public void Write(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
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
            IncomingPush,
            IncomingPull,
            OutgoingPush,
            OutgoingPull
        }

        public interface IReplicationPerformanceStats
        {
            void Write(JsonOperationContext context, AbstractBlittableJsonTextWriter writer);
        }
    }
}
