using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Client.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public enum ReplicationStatus
    {
        Sending,
        Received,
        Failed
    }

    public class ReplicationStatistics
    {
        private const int MaxEntries = 1024;
        public ConcurrentQueue<IStatsEntry> OutgoingStats = new ConcurrentQueue<IStatsEntry>();
        public ConcurrentQueue<IStatsEntry> IncomingStats = new ConcurrentQueue<IStatsEntry>();
        public Queue<IStatsEntry> ResolverStats = new Queue<IStatsEntry>();

        private readonly DocumentReplicationLoader _loader;

        public ReplicationStatistics(DocumentReplicationLoader loader)
        {
            _loader = loader;
        }

        public DynamicJsonValue LiveStats()
        {
            var outgoingHeartBeats = new DynamicJsonValue();
            var incomingHeartBeats = new DynamicJsonValue();
            var outgoingReplicationHandlers = _loader.OutgoingHandlers ?? Enumerable.Empty<OutgoingReplicationHandler>();

            foreach (var o in outgoingReplicationHandlers)
            {
                outgoingHeartBeats[o.FromToString] = new DateTime(o.LastHeartbeatTicks);
            }

            var incomingReplicationHandlers = _loader.IncomingHandlers ?? Enumerable.Empty<IncomingReplicationHandler>();
            foreach (var i in incomingReplicationHandlers)
            {
                incomingHeartBeats[i.FromToString] = new DateTime(i.LastHeartbeatTicks);
            }

            return new DynamicJsonValue
            {
                ["SampledAt"] = DateTime.UtcNow,
                ["OutgoingHeartbeats"] = outgoingHeartBeats,
                ["IncomingHeartbeats"] = incomingHeartBeats,
                ["ConflictResolverStatus"] = _loader.ResolveConflictsTask.Status.ToString(),
                ["ConflictsCount"] = _loader.ConflictsCount
            };
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
            public int DocumentsCount;
            public long SentEtagMin;
            public long SentEtagMax;
            public string Exception;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    ["Destination"] = Destination,
                    ["Status"] = Status,
                    ["Message"] = Message,
                    ["StartSendingTime"] = StartSendingTime,
                    ["EndSendingTime"] = EndSendingTime,
                    ["DocumentsCount"] = DocumentsCount,
                    ["SentEtagMin"] = SentEtagMin,
                    ["SentEtagMax"] = SentEtagMax,
                    ["Exception"] = Exception
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
            public int DocumentsCount;
            public long RecievedEtag;
            public string Exception;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    ["Source"] = Source,
                    ["Status"] = Status,
                    ["Message"] = Message,
                    ["RecievedTime"] = RecievedTime,
                    ["CompletedTime"] = DoneReplicateTime,
                    ["DocumentsCount"] = DocumentsCount,
                    ["RecievedEtag"] = RecievedEtag,
                    ["Exception"] = Exception
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
