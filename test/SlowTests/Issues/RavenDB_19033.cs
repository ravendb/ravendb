using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19033 : NoDisposalNeeded
{
    public RavenDB_19033(ITestOutputHelper output) : base(output)
    {
    }

    private class MyDb : IDisposable
    {
        public static List<MyDb> RunningDatabases = new List<MyDb>();

        public MyDb(string name)
        {
        }

        public void Run()
        {
            RunningDatabases.Add(this);
        }

        public void Dispose()
        {
            RunningDatabases.Remove(this);
        }
    }

    [Fact]
    public async Task UnloadAndLockDatabaseMustNotIgnoreDatabaseDisabledLock()
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
            task1.Wait();
        }

        using (dbsCache.RemoveLockAndReturn(dbName, x => x.Dispose(), out _))
        {
            await Assert.ThrowsAsync<DatabaseDisabledException>(async () =>
            {
                using (await DatabasesLandlord.UnloadAndLockDatabaseImpl(dbsCache, dbName, x => x.Dispose(), "test"))
                {

                }
            });

            var task2 = new Task<MyDb>(() =>
            {
                var myDb = new MyDb(dbName);

                myDb.Run();

                return myDb;
            }, TaskCreationOptions.RunContinuationsAsynchronously);

            var database2 = dbsCache.GetOrAdd(dbName, task2); // might be run from different thread

            if (database2 == task2)
            {
                Assert.False(true, "we should never be allowed to start a new database since the database disabled lock created by RemoveLockAndReturn() is still acquired");

                task2.Start();
                task2.Wait();
            }
        }

        // delete database - there must be no running db instance

        Assert.Empty(MyDb.RunningDatabases);
    }
}
