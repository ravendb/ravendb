using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests;

public class RavenDB_19654 : RachisConsensusTestBase
{
    public RavenDB_19654(ITestOutputHelper output) : base(output)
    {
    }

    private Dictionary<string, LogInfo> TestData = new Dictionary<string, LogInfo>
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

            LastCommittedTerm(leader, context, TestData["A"]);
            LastCommittedTerm(follower, context2, TestData["B"]);

            var basePath = "/home/haludi/work/ravendb/RavenDB-19654/Logs";
             var currentDir = DateTime.Now.ToString("yyyyMMdd-hhmmss");
             LoggingSource.Instance.SetupLogMode(LogMode.Information, Path.Combine(basePath, currentDir), TimeSpan.MaxValue, long.MaxValue, false);
            
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

    private static void LastCommittedTerm(RachisConsensus<CountingStateMachine> node, ClusterOperationContext context, LogInfo info)
    {
        node.ClearAppendedEntriesAfter(context, 0);
        var lastCommittedTerm = -1L;

        foreach (var (term, index) in info)
        {
            if (info.CommitIndex == index)
                lastCommittedTerm = term;

            var cmd = new TestCommand {Name = "test", Value = 1};
            RachisConsensus.TestingStuff.InsertToLogDirectly(context, node, term, index, cmd, RachisEntryFlags.StateMachineCommand);
        }

        RachisConsensus.TestingStuff.UpdateStateDirectly(context, node, info.CommitIndex, lastCommittedTerm, info.LastTruncatedIndex, info.LastTruncatedTerm);
    }

    private class LogInfo : IEnumerable<(long Term, long Index)>
    {
        public long CommitIndex;
        public long LastTruncatedIndex;
        public long LastTruncatedTerm;
        public long FirstEntryIndex;
        public long LastLogEntryIndex;

        public (long Index, long Term)[] Entries;
        public IEnumerator<(long Term, long Index)> GetEnumerator()
        {
            var entries = Entries;
            for (int i = 0; i < entries.Length; i++)
            {
                var firstInTerm = entries[i];
                var firstInNextTerm = i + 1 < entries.Length ? entries[i + 1].Index - 1 : LastLogEntryIndex;
                for (long j = firstInTerm.Index; j <= firstInNextTerm; j++)
                {
                    yield return (firstInTerm.Term, j);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
