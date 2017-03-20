using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public enum ReplicationStatus
    {
        Sending,
        Received,
        Failed
    }

    public class LiveStats
    {
        public DateTime SampledAt;
        public Dictionary<string, DateTime> OutgoingHeartbeats = new Dictionary<string, DateTime>();
        public Dictionary<string, DateTime> IncomingHeartbeats = new Dictionary<string, DateTime>();
        public string ConflictResolverStatus;
        public long ConflictsCount;

        public void Sample(DocumentReplicationLoader loader, ReplicationStatistics stats)
        {
            OutgoingHeartbeats.Clear();
            IncomingHeartbeats.Clear();

            SampledAt = DateTime.UtcNow;
            ConflictsCount = loader.ConflictResolver.ConflictsCount;
            ConflictResolverStatus = loader.ConflictResolver.ResolveConflictsTask.Status.ToString();
            var outgoingReplicationHandlers = loader.OutgoingHandlers ?? Enumerable.Empty<OutgoingReplicationHandler>();

            foreach (var o in outgoingReplicationHandlers)
            {
                OutgoingHeartbeats[o.FromToString] = new DateTime(o.LastHeartbeatTicks);
            }

            var incomingReplicationHandlers = loader.IncomingHandlers ?? Enumerable.Empty<IncomingReplicationHandler>();
            foreach (var i in incomingReplicationHandlers)
            {
                IncomingHeartbeats[i.FromToString] = new DateTime(i.LastHeartbeatTicks);
            }
        }
    }

    public class ReplicationStatistics
    {
        private const int MaxEntries = 1024;
        public ConcurrentQueue<IStatsEntry> OutgoingStats = new ConcurrentQueue<IStatsEntry>();
        public ConcurrentQueue<IStatsEntry> IncomingStats = new ConcurrentQueue<IStatsEntry>();
        public Queue<IStatsEntry> ResolverStats = new Queue<IStatsEntry>();
        public LiveStats CurrentStats = new LiveStats();

        private readonly DocumentReplicationLoader _loader;
        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1);

        public ReplicationStatistics(DocumentReplicationLoader loader)
        {
            _loader = loader;
        }

        public void Update()
        {
            _locker.Wait(5);
            try
            {
                CurrentStats.Sample(_loader, this);
            }
            finally
            {
                _locker.Release();
            }           
        }

        public interface IStatsEntry
        {
            DynamicJsonValue ToJson();
        }

        public class OutgoingBatchStats : IStatsEntry
        {
            public string Destination;
            public ReplicationStatus Status;
            public string Message;
            public DateTime StartSendingTime;
            public DateTime EndSendingTime;
            public int ItemsCount;
            public int AttachmentStreamsCount;
            public long SentEtagMin;
            public long SentEtagMax;
            public string Exception;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Destination)] = Destination,
                    [nameof(Status)] = Status,
                    [nameof(Message)] = Message,
                    [nameof(StartSendingTime)] = StartSendingTime,
                    [nameof(EndSendingTime)] = EndSendingTime,
                    [nameof(ItemsCount)] = ItemsCount,
                    [nameof(AttachmentStreamsCount)] = AttachmentStreamsCount,
                    [nameof(SentEtagMin)] = SentEtagMin,
                    [nameof(SentEtagMax)] = SentEtagMax,
                    [nameof(Exception)] = Exception,
                };
            }
        }

        public class IncomingBatchStats : IStatsEntry
        {
            public string Source;
            public ReplicationStatus Status;
            public string Message;
            public DateTime RecievedTime;
            public DateTime DoneReplicateTime;
            public int ItemsCount;
            public int AttachmentStreamsCount;
            public long RecievedEtag;
            public string Exception;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Source)] = Source,
                    [nameof(Status)] = Status,
                    [nameof(Message)] = Message,
                    [nameof(RecievedTime)] = RecievedTime,
                    ["CompletedTime"] = DoneReplicateTime,
                    [nameof(ItemsCount)] = ItemsCount,
                    [nameof(AttachmentStreamsCount)] = AttachmentStreamsCount,
                    [nameof(RecievedEtag)] = RecievedEtag,
                    [nameof(Exception)] = Exception,
                };
            }
        }

        public class ResolverIterationStats : IStatsEntry
        {
            public DateTime StartTime;
            public DateTime EndTime;
            public long ConflictsLeft;
            public DatabaseResolver DefaultResolver;
            internal Dictionary<string, int> ResolvedBy;

            public void AddResolvedBy(string by, int count)
            {
                if (ResolvedBy == null)
                {
                    ResolvedBy = new Dictionary<string, int>();
                }
                int value;
                ResolvedBy.TryGetValue(by, out value);
                ResolvedBy[by] = count + value;
            }

            public DynamicJsonValue ToJson()
            {
                var resolvedBy = new DynamicJsonValue();
                if (ResolvedBy != null)
                {
                    foreach (var kvp in ResolvedBy)
                    {
                        resolvedBy[kvp.Key] = kvp.Value;
                    }    
                }
                
                return new DynamicJsonValue
                {
                    ["StartTime"] = StartTime,
                    ["EndTime"] = EndTime,
                    ["ConflictsLeft"] = ConflictsLeft,
                    ["DatabaseResolver"] = DefaultResolver?.ToJson(),
                    ["ResolvedBy"] = resolvedBy
                };
            }
        }

        public void Add(IncomingBatchStats stats)
        {
            while (IncomingStats.Count >= MaxEntries)
            {
                IStatsEntry outStats;
                IncomingStats.TryDequeue(out outStats);
            }
            // Theoretically, it could happend that we have more than 'MaxEntries' entries. But we don't care,
            // becuase we are still bounded and in the next call of this function we will be within 'MaxEntries' again.  
            IncomingStats.Enqueue(stats);
        }

        public void Add(OutgoingBatchStats stats)
        {
            while (OutgoingStats.Count >= MaxEntries)
            {
                IStatsEntry outStats;
                OutgoingStats.TryDequeue(out outStats);
            }
            OutgoingStats.Enqueue(stats);
        }

        public void Add(ResolverIterationStats stats)
        {
            while (ResolverStats.Count >= MaxEntries)
            {
                ResolverStats.Dequeue();
            }
            ResolverStats.Enqueue(stats);
        }
    }
}
