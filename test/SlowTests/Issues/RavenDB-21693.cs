using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
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
        public void ShouldNotLockDatabaseForever()
        {
            List<MyDb> runningDatabases = new List<MyDb>();
            var dbsCache = new ResourceCache<MyDb>();

            var dbName = "foo";

            var task1 = new Task<MyDb>(() =>
            {
                var myDb = new MyDb(dbName, runningDatabases);

                myDb.Run();

                return myDb;
            }, TaskCreationOptions.RunContinuationsAsynchronously);

            var database1 = dbsCache.GetOrAdd(dbName, task1);

            if (database1 == task1)
            {
                task1.Start();
                task1.Wait();
            }
            Assert.NotEmpty(runningDatabases);

            var mre = new ManualResetEvent(false);
            var mre2 = new ManualResetEvent(false);
            Task t = null;
            dbsCache.ForTestingPurposesOnly().OnUnlikelyTryGet = () =>
            {
                mre2.Set();
                mre.WaitOne();
            };

            using (dbsCache.RemoveLockAndReturn(dbName, x =>
                   {
                       x.Dispose();
                       t = new Task(() => dbsCache.TryGetValue(dbName, out _), TaskCreationOptions.LongRunning);
                       t.Start();
                   }, out _))
            {

                mre2.WaitOne();
            }

            mre.Set();

            t.Wait();

            Assert.False(dbsCache.TryGetValue(dbName, out var task));
            Assert.Empty(runningDatabases);
        }


        [RavenFact(RavenTestCategory.Core)]
        public void ShouldUseNewDatabaseIfItWasChanged()
        {
            List<MyDb> runningDatabases = new List<MyDb>();
            var dbsCache = new ResourceCache<MyDb>();

            var dbName = "foo";

            var task1 = new Task<MyDb>(() =>
            {
                var myDb = new MyDb(dbName, runningDatabases);

                myDb.Run();

                return myDb;
            }, TaskCreationOptions.RunContinuationsAsynchronously);

            var database1 = dbsCache.GetOrAdd(dbName, task1);

            if (database1 == task1)
            {
                task1.Start();
                task1.Wait();
            }

            var mre = new ManualResetEvent(false);
            var mre2 = new ManualResetEvent(false);
            Task t = null;
            Task<MyDb> past = null;
            dbsCache.ForTestingPurposesOnly().OnUnlikelyTryGet = () =>
            {
                mre2.Set();
                mre.WaitOne();
            };

            using (dbsCache.RemoveLockAndReturn(dbName, x =>
                   {
                       x.Dispose();
                       t = new Task(() => dbsCache.TryGetValue(dbName, out past), TaskCreationOptions.LongRunning);
                       t.Start();
                   }, out _))
            {

                mre2.WaitOne();
            }
            // create new database meanwhile
            var expected = new Task<MyDb>(() =>
            {
                var myDb = new MyDb(dbName, runningDatabases);

                myDb.Run();

                return myDb;
            }, TaskCreationOptions.RunContinuationsAsynchronously);

            var database2 = dbsCache.GetOrAdd(dbName, expected);
            if (database2 == expected)
            {
                expected.Start();
                expected.Wait();
            }

            mre.Set();

            t.Wait();

            // when we called dbsCache.TryGetValue(dbName, out past) method the task in resourceCache was the faulty one created in RemoveLockAndReturn()
            // but we should have the expected one (database2) after the call returns

            Assert.True(past.IsCompletedSuccessfully, "past.IsCompletedSuccessfully");
            Assert.Equal(expected, past);

            Assert.True(dbsCache.TryGetValue(dbName, out var current));
            Assert.Equal(expected, past);
            Assert.Equal(current, past);
            Assert.NotEmpty(runningDatabases);

        }
    }
}
