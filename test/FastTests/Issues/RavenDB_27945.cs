using System.Threading;
using Raven.Server.Utils;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_27945 : NoDisposalNeeded
    {
        public RavenDB_27945(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void NewPooledThreadMustNotInheritTheAsyncLocalsOfItsCreator()
        {
            // Index.Start starts the indexing thread while its AsyncLocal "write lock taken" flag is set
            var flag = new AsyncLocal<bool> { Value = true };
            bool? seenOnPooledThread = null;

            // a new pool is empty, so LongRunning has to create a new thread
            using (var pool = new PoolOfThreads())
            {
                var work = pool.LongRunning(_ => seenOnPooledThread = flag.Value, null,
                    ThreadNames.ForIndex("Indexing of Orders/ByName of db", "Orders/ByName", "db"));

                Assert.True(work.Join(30_000));
            }

            Assert.False(seenOnPooledThread);
        }
    }
}
