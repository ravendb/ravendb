using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Database;
using Raven.Server.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19033 : NoDisposalNeeded
{
    public RavenDB_19033(ITestOutputHelper output) : base(output)
    {
    }

    internal class MyDb : IDisposable
    {
        private readonly List<MyDb> _runningDatabases;

        public MyDb(string name, List<MyDb> runningDatabases)
        {
            _runningDatabases = runningDatabases;
        }

        public void Run()
        {
            _runningDatabases.Add(this);
        }

        public void Dispose()
        {
            _runningDatabases.Remove(this);
        }
    }

    [Fact]
    public async Task UnloadAndLockDatabaseMustNotIgnoreDatabaseDisabledLock()
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
            await task1;
        }
        Assert.NotEmpty(runningDatabases);

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
                var myDb = new MyDb(dbName, runningDatabases);

                myDb.Run();

                return myDb;
            }, TaskCreationOptions.RunContinuationsAsynchronously);

            var database2 = dbsCache.GetOrAdd(dbName, task2); // might be run from different thread

            if (database2 == task2)
            {
                Assert.Fail("we should never be allowed to start a new database since the database disabled lock created by RemoveLockAndReturn() is still acquired");

                task2.Start();
                await task2.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        // delete database - there must be no running db instance

        Assert.Empty(runningDatabases);
    }
}
