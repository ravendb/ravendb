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

public class LogDivergenceTests : RachisConsensusTestBase
{
    public LogDivergenceTests(ITestOutputHelper output) : base(output)
    {
    }

    private const string LeaderTag = "A";
    private const string FollowerTag = "B";
    
    [Fact]
    public async Task RavenDB_19654()
    {
        var testData = new Dictionary<string, LogInfo>
        {
            [LeaderTag] = new LogInfo
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
            [FollowerTag] = new LogInfo
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
        await CanHandleLogDivergence(testData);
    }

    [Fact]
    public async Task RDBS_12738()
    {
        var testData = new Dictionary<string, LogInfo>
        {
            [LeaderTag] = new LogInfo
            {
                CommitIndex = 218996270,
                LastTruncatedIndex = 218995269,
                LastTruncatedTerm = 155852,
                FirstEntryIndex = 218995270,
                LastLogEntryIndex = 218996270,
                Entries = new (long Index, long Term)[]
                {
                    (Index: 218995270, Term: 155852), 
                    (Index: 218995341, Term: 155853),
                    //      218995355,       155853
                    (Index: 218995381, Term: 155856), 
                    (Index: 218995953, Term: 155858),
                }
            },
            [FollowerTag] = new LogInfo
            {
                CommitIndex = 218995269,
                LastTruncatedIndex = 218995145,
                LastTruncatedTerm = 155851,
                FirstEntryIndex = 218995146,
                LastLogEntryIndex = 218995356,
                Entries = new (long Index, long Term)[]
                {
                    (Index: 218995146, Term: 155851), 
                    (Index: 218995254, Term: 155852), 
                    (Index: 218995341, Term: 155853),
                    //      218995355,      155853
                    (Index: 218995356, Term: 155855),
                }
            }
        };
        await CanHandleLogDivergence(testData);
    }

    [Fact]
    public async Task RavenDB_20192()
    {
        var testData = new Dictionary<string, LogInfo>
        {
            [LeaderTag] = new LogInfo
            {
                CommitIndex = 315961,
                LastTruncatedIndex = 303165,
                LastTruncatedTerm = 2142,
                FirstEntryIndex = 303166,
                LastLogEntryIndex = 315961,
                Entries = new (long Index, long Term)[]
                {
                    (Index: 303166, Term: 2142), 
                    (Index: 303167, Term: 2146), 
                    (Index: 303168, Term: 2148), 
                    (Index: 303176, Term: 2153),
                }
            },
            [FollowerTag] = new LogInfo
            {
                CommitIndex = 303165,
                LastTruncatedIndex = 303148,
                LastTruncatedTerm = 2136,
                FirstEntryIndex = 303149,
                LastLogEntryIndex = 303168,
                Entries = new (long Index, long Term)[]
                {
                    (Index: 303149, Term: 2136), 
                    (Index: 303154, Term: 2137), 
                    (Index: 303155, Term: 2138), 
                    (Index: 303156, Term: 2139), 
                    (Index: 303157, Term: 2142),
                    //      303166,       2142), 
                    (Index: 303167, Term: 2143), 
                    (Index: 303168, Term: 2145),
                }
            }
        };
        await CanHandleLogDivergence(testData);
    }

    private async Task CanHandleLogDivergence(Dictionary<string, LogInfo> testData)
    {
        var leader = await CreateNetworkAndGetLeader(2);
        var follower = GetFollowers().Single();

        using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (follower.ContextPool.AllocateOperationContext(out ClusterOperationContext context2))
        using (var tx1 = context.OpenWriteTransaction())
        using (var tx2 = context2.OpenWriteTransaction())
        {
            leader.CurrentLeader?.StepDown(forceElection: false);

            InsertAndSetLogState(leader, context, testData[LeaderTag]);
            InsertAndSetLogState(follower, context2, testData[FollowerTag]);

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
        public long CommitIndex { init; get; }
        public long LastTruncatedIndex { init; get; }
        public long LastTruncatedTerm { init; get; }
        public long FirstEntryIndex { init; get; }
        public long LastLogEntryIndex { init; get; }
        public (long Index, long Term)[] Entries { init; get; }

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
