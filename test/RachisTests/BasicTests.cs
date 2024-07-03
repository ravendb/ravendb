using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class BasicTests : RachisConsensusTestBase
    {
        public BasicTests(ITestOutputHelper output) : base(output)
        {
        }
        
        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task CanApplyCommitAcrossAllCluster(int amount)
        {
            var leader = await CreateNetworkAndGetLeader(amount);
            long lastIndex = 0;
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 5; i++)
                {
                    long index = 0;
                    await ActionWithLeader(async l =>
                    {
                        (index, _) = await l.PutAsync(new TestCommand { Name = "test", Value = i });
                    });
                    lastIndex = index;
                }
            }

            foreach (var r in RachisConsensuses)
            {
                var condition = await
                    r.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex)
                        .WaitWithoutExceptionAsync(5000);

                using (r.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var commitIndex = r.GetLastCommitIndex(context);
                    Assert.True(condition, $"Last commit is {commitIndex} wanted {lastIndex}");
                }
            }

        }

        [Fact]
        public void RavenDB_13659()
        {
            EnableCaptureWriteTransactionStackTrace = true;

            var leader = AsyncHelpers.RunSync(() => CreateNetworkAndGetLeader(1));
            var mre = new ManualResetEventSlim();
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            var currentThread = NativeMemory.CurrentThreadStats.ManagedThreadId;

            leader.Timeout.Start(() =>
            {
                if (currentThread == NativeMemory.CurrentThreadStats.ManagedThreadId)
                    throw new InvalidOperationException("Can't use same thread as the xUnit test");

                mre.Set();
                try
                {
                    using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenWriteTransaction())
                    {

                    }
                    tcs.TrySetResult(null);
                }
                catch (Exception e)
                {
                    tcs.TrySetException(e);
                }
            });

            using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenWriteTransaction())
            {
                mre.Wait();
                leader.SetNewStateInTx(context, RachisState.Follower, null, leader.CurrentTermIn(context), "deadlock");
                context.Transaction.Commit();
            }

            AsyncHelpers.RunSync(() => tcs.Task);
        }
    }
}
