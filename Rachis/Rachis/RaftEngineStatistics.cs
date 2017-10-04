using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Rachis.Messages;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Rachis
{
    public class RaftEngineStatistics
    {
        public const int NumberOfHeartbeatsToTrack = 50;
        public const int NumberOfTimeoutsToTrack = 5;
        public const int NumberOfMessagesToTrack = 100;
        public const int NumberOfElectionsToTrack = 5;
        public const int NumberOfCommitsToTrack = 10;

        public RaftEngineStatistics(RaftEngine engine)
        {
            Name = engine.Options.Name;
            ElectionTimeout = engine.Options.ElectionTimeout;
            HeartbeatTimeout = engine.Options.HeartbeatTimeout;
            MaxEntriesPerRequest = engine.Options.MaxEntriesPerRequest;
            MaxLogLengthBeforeCompaction = engine.Options.MaxLogLengthBeforeCompaction;
            HeartBeats = new ConcurrentQueue<DateTime>();
            TimeOuts = new ConcurrentQueue<TimeoutInformation>();
            Messages = new ConcurrentQueue<MessageWithTimingInformation>();
            Elections = new ConcurrentQueue<ElectionInformation>();
            CommitTimes = new ConcurrentQueue<CommitInformation>();
        }

        public LogEntry LastLogEntry { get; set; }
        public List<LogEntry> LastLogsEntries { get; set; }
        public long CommitIndex { get; set; }
        private ConcurrentDictionary<long, DateTime> IndexesToAppendTimes = new ConcurrentDictionary<long, DateTime>(); 
        public ConcurrentQueue<TimeoutInformation> TimeOuts { get; }
        public ConcurrentQueue<ElectionInformation> Elections { get; } 
        public ConcurrentQueue<DateTime> HeartBeats { get; }
        public ConcurrentQueue<MessageWithTimingInformation> Messages { get; }
        public ElectionInformation LastElectionInformation => Elections.LastOrDefault();
        public ConcurrentQueue<CommitInformation> CommitTimes { get; } 

        public Topology CurrenTopology { get; set; }

        public string Name { get; set; }

        public int MaxLogLengthBeforeCompaction { get; set; }

        public int MaxEntriesPerRequest { get; set; }

        public int HeartbeatTimeout { get; set; }

        public int ElectionTimeout { get; set; }
        public string CurrentLeader { get; set; }
        public long CurrentTerm { get; set; }
        public FollowerLastSentEntries FollowersStatistics = null;
        public void ReportIndexAppend(long index)
        {
            IndexesToAppendTimes[index] = DateTime.UtcNow;
        }
        public void ReportCommitIndex(long index)
        {
            DateTime time;
            if (IndexesToAppendTimes.TryRemove(index, out time))
            {
                var duration = (DateTime.UtcNow - time).TotalSeconds;
                CommitTimes.LimitedSizeEnqueue(new CommitInformation() {AppendTime = time,CommitDurationInSeconds = duration ,Index = index}
                ,NumberOfCommitsToTrack);
            }            
        }
    }

    public class FollowerLastSentEntries
    {
        public FollowerLastSentEntries(ConcurrentDictionary<string, long> f2e)
        {
            FollowersToLastSent = f2e;
        }
        public readonly ConcurrentDictionary<string,long> FollowersToLastSent;
        public long MaxQuorumIndex { get; set; }
    }

    public class TimeoutInformation
    {
        public DateTime TimeOutTime { get; set; }
        public RaftEngineState State { get; set; }
        public int Timeout { get; set; }
        public int ActualTimeout { get; set; }
    }

    public class ElectionInformation
    {
        private ConcurrentQueue<RequestVoteResponse> votes = new ConcurrentQueue<RequestVoteResponse>();
        public DateTime StartTime { get; set; }
        public long CurrentTerm { get; set; }
        public bool WonTrialElection { get; set; }
        public bool TermIncreaseMightGetMyVote { get; set; }
        public bool ForcedElection { get; set; }
        public IEnumerable<NodeConnectionInfo> VotingNodes { get; set; }
        public ConcurrentQueue<RequestVoteResponse> Votes => votes;
    }

    public class CommitInformation
    {
        public long Index { get; set; }
        public DateTime AppendTime { get; set; }
        public double CommitDurationInSeconds { get; set; }
    }

    public class MessageWithTimingInformation
    {
        public DateTime MessageReceiveTime { get; set; }
        public BaseMessage Message { get; set; }
    }
}
