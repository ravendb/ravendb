using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Issues.RavenDB_19033;

namespace SlowTests.Issues
{
    public class RavenDB_21693 : NoDisposalNeeded
    {
        public RavenDB_21693(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task ShouldNotLockDatabaseForever()
        {
            var dbsCache = new ResourceCache<MyDb>();

            var dbName = "foo";

            var task1 = new Task<MyDb>(() =>
            {
                var myDb = new MyDb(dbName);

                myDb.Run();

                return myDb;
            }, TaskCreationOptions.RunContinuationsAsynchronously);

            var database1 = dbsCache.GetOrAdd(dbName, task1);

            if (database1 == task1)
            {
                task1.Start();
                await task1;
            }

            var mre = new AsyncManualResetEvent();
            var mre2 = new AsyncManualResetEvent();
            Task t = null;
            dbsCache.ForTestingPurposesOnly().OnUnlikelyTryGet = () =>
            {
                mre2.Set();
                mre.WaitAsync().GetAwaiter().GetResult();
            };

            using (dbsCache.RemoveLockAndReturn(dbName, x =>
                   {
                       x.Dispose();
                       t = Task.Run(() => dbsCache.TryGetValue(dbName, out _));
                   }, out _))
            {

                await mre2.WaitAsync();
            }

            mre.Set();

            await t;

            Assert.False(dbsCache.TryGetValue(dbName, out var task));
        }
    }
}
