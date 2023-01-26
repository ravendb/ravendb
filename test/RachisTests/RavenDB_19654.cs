using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests;

public class RavenDB_19654 : RachisConsensusTestBase
{
    public RavenDB_19654(ITestOutputHelper output) : base(output)
    {
    }

    private readonly Dictionary<string, LogInfo> _testData = new Dictionary<string, LogInfo>
    {
        ["A"] = new LogInfo
        {
            CommitIndex = 664033464,
            LastTruncatedIndex = 664033448,
            LastTruncatedTerm = 1163367,
            FirstEntryIndex = 664033449,
            LastLogEntryIndex = 664033464,
            Entries = new (long Index, long Term)[]
            {
                (Index: 664033449, Term: 1163368),
                (Index: 664033454, Term: 1163370),
            }
        },
        ["B"] = new LogInfo
        {
            CommitIndex = 664033453,
            LastTruncatedIndex = 664033430,
            LastTruncatedTerm = 1163367,
            FirstEntryIndex = 664033431,
            LastLogEntryIndex = 664033464,
            Entries = new (long Index, long Term)[]
            {
                (Index: 664033431, Term: 1163367),
                (Index: 664033449, Term: 1163368),
            }
        }
    };
    
    [Fact]
    public async Task CanHandleLogDivergence()
    {
        var leader = await CreateNetworkAndGetLeader(2);
        var follower = GetFollowers().Single();

        using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (follower.ContextPool.AllocateOperationContext(out ClusterOperationContext context2))
        using (var tx1 = context.OpenWriteTransaction())
        using (var tx2 = context2.OpenWriteTransaction())
        {
            leader.CurrentLeader?.StepDown(forceElection: false);

            InsertAndSetLogState(leader, context, _testData["A"]);
            InsertAndSetLogState(follower, context2, _testData["B"]);
            
            tx2.Commit();
            tx1.Commit();
        }

        long lastIndex;
        using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            lastIndex = leader.GetLastCommitIndex(context);
        }

        await follower.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex);
    }

    private static void InsertAndSetLogState(RachisConsensus<CountingStateMachine> node, ClusterOperationContext context, LogInfo info)
    {
        node.ClearAppendedEntriesAfter(context, 0);
        var lastCommittedTerm = -1L;

        foreach (var (term, index) in info)
        {
            if (info.CommitIndex == index)
                lastCommittedTerm = term;

            var cmd = new TestCommand {Name = "test", Value = 1};
            RachisConsensus.TestingStuff.InsertToLogDirectlyForDebug(context, node, term, index, cmd, RachisEntryFlags.StateMachineCommand);
        }

        RachisConsensus.TestingStuff.UpdateStateDirectlyForDebug(context, node, info.CommitIndex, lastCommittedTerm, info.LastTruncatedIndex, info.LastTruncatedTerm);
    }

    private class LogInfo : IEnumerable<(long Term, long Index)>
    {
        public long CommitIndex { init ; get; }
        public long LastTruncatedIndex { init ; get; }
        public long LastTruncatedTerm { init ; get; }
        public long FirstEntryIndex { init ; get; }
        public long LastLogEntryIndex { init ; get; }
        public (long Index, long Term)[] Entries { init ; get; }
        public IEnumerator<(long Term, long Index)> GetEnumerator()
        {
            for (int i = 0; i < Entries.Length; i++)
            {
                var (firstIndexInTerm, term) = Entries[i];
                var lastIndexInTerm = i + 1 < Entries.Length ? Entries[i + 1].Index - 1 : LastLogEntryIndex;
                for (long j = firstIndexInTerm; j <= lastIndexInTerm; j++)
                {
                    yield return (term, j);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
