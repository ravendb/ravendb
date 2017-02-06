using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Collections.LockFree;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Replication
{
    public enum ReplicationStatus
    {
        Heartbeat,
        Sending,
        Received,
        Accepted,
        Rejected,
        Failed,
        Succeed,
        Disabled
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
            _loader.OutgoingHandlers?.ForEach(o => outgoingHeartBeats[o.DestinationDbId] = o.LastHeartbeatTicks);
            _loader.IncomingHandlers?.ForEach(i => incomingHeartBeats[i.ConnectionInfo.SourceDatabaseId] = i.LastHeartbeatTicks);

            return new DynamicJsonValue
            {
                ["SampledAt"] = DateTime.UtcNow,
                ["OutgoingHeartbeats"] = outgoingHeartBeats,
                ["IncomingHeartbeats"] = incomingHeartBeats,
                ["ConflictResolverStatus"] = _loader.ResolveConflictsTask.Status == TaskStatus.Running ? "Running" : "Not Running",
                ["ConflictsCount"] = _loader.ConflictsCount
            };
        }

        public interface IStatsEntry
        {
            DynamicJsonValue ToJson();
        }

        public struct OutgoingBatchStats : IStatsEntry
        {
            public string Destination ;
            public ReplicationStatus Status ;
            public string Message;
            public DateTime StartSendingTime ;
            public DateTime EndSendingTime ;
            public int DocumentsCount ;
            public long SentEtagMin ;
            public long SentEtagMax ;

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
                    ["SentEtagMax"] = SentEtagMax
                };
            }
        }

        public struct IncomingBatchStats : IStatsEntry
        {           
            public string Source ;
            public ReplicationStatus Status ;
            public string Message;
            public DateTime RecievedTime ;
            public DateTime DoneReplicateTime ;
            public int DocumentsCount ;
            public long RecievedEtag ;

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
                    ["RecievedEtag"] = RecievedEtag
                };
            }
        }

        public struct ResolverIterationStats : IStatsEntry
        {
            public DateTime StartTime;
            public DateTime EndTime;
            public long ConflictsLeft;
            public DatabaseResolver DefaultResolver;
            private Dictionary<string, int> _resolvedBy;

            public void AddResolvedBy(string by, int count)
            {
                if (_resolvedBy == null)
                {
                    _resolvedBy = new Dictionary<string, int>();
                }
                _resolvedBy[@by] = _resolvedBy.ContainsKey(@by) ? +count : count;
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    ["StartTime"] = StartTime,
                    ["EndTime"] = EndTime,
                    ["ConflictsLeft"] = ConflictsLeft,
                    ["DatabaseResolver"] = DefaultResolver?.ToJson(),
                    ["ResolvedBy"] = _resolvedBy
                };
            }
        }

        public enum StatsType
        {
            IncomingBatchStats,
            OutgoingBatchStats,
            ResolverIterationStats
        }

        private readonly IDictionary<Type, StatsType> _typeDictonary = new Dictionary<Type, StatsType>
        {
            {typeof(IncomingBatchStats),StatsType.IncomingBatchStats},
            {typeof(OutgoingBatchStats),StatsType.OutgoingBatchStats},
            {typeof(ResolverIterationStats),StatsType.ResolverIterationStats}
        };

        public void Add<T>(T stats) where T: IStatsEntry
        {
            switch (_typeDictonary[typeof(T)])
            {
                case StatsType.IncomingBatchStats:
                    while (IncomingStats.Count >= MaxEntries)
                    {
                        IStatsEntry outStats;
                        IncomingStats.TryDequeue(out outStats);
                    }
                    // Theoretically, it could happend that we have more than 'MaxEntries' entries. But we don't care,
                    // becuase we are still bounded and in the next call of this function we will be within 'MaxEntries' again.  
                    IncomingStats.Enqueue(stats);
                    break;
                case StatsType.OutgoingBatchStats:
                    while (OutgoingStats.Count >= MaxEntries)
                    {
                        IStatsEntry outStats;
                        OutgoingStats.TryDequeue(out outStats);
                    }
                    OutgoingStats.Enqueue(stats);
                    break;
                case StatsType.ResolverIterationStats:
                    while (ResolverStats.Count >= MaxEntries)
                    {
                        ResolverStats.Dequeue();
                    }
                    ResolverStats.Enqueue(stats);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }   
    }
}
