using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.RecoveryTests
{
    public class SampleDatabaseRecovery : RecoveryTestBase
    {
        public SampleDatabaseRecovery(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformFact(RavenArchitecture.AllX64, Skip = "RavenDB-13765")]
        public async Task CanRecoverSampleData()
        {
            var rootPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            var badName = GetDatabaseName();
            var badPath = Path.Combine(rootPath, badName);
            var recoveredPath = Path.Combine(rootPath, "recovery");

            using (var corruptedStore = GetDocumentStore(
                new Options
                {
                    Path = badPath,
                    RunInMemory = false,
                    DeleteDatabaseOnDispose = false,
                    ModifyDatabaseName = _ => badName
                }))
            {
                Samples.CreateNorthwindDatabase(corruptedStore);
            }

            Assert.True(Server.ServerStore.DatabasesLandlord.UnloadDirectly(badName));

            var recoveryOptions = new RecoveryOptions
            {
                PathToDataFile = badPath,
                RecoveryDirectory = recoveredPath,
            };

            using (var store = await RecoverDatabase(recoveryOptions))
            {
            }
        }

        [MultiplatformFact(RavenArchitecture.AllX64)]
        public async Task CanRecoverTimeSeries()
        {
            var rootPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            var badName = GetDatabaseName();
            var badPath = Path.Combine(rootPath, badName);
            var recoveredPath = Path.Combine(rootPath, "recovery");

            using (var serverStore = GetDocumentStore(new Options { CreateDatabase = false }))
            using (Databases.EnsureDatabaseDeletion(badName, serverStore))
            {
                using (var store = GetDocumentStore(
                    new Options
                    {
                        Path = badPath,
                        RunInMemory = false,
                        DeleteDatabaseOnDispose = false,
                        ModifyDatabaseName = _ => badName
                    }))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = new User { Name = "karmel" };
                        await session.StoreAsync(user, "Users/karmel");
                        var ts = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>(user);
                        for (int i = 0; i < 1000; i++)
                        {
                            ts.Append(DateTime.UtcNow.AddMinutes(i), new TimeSeriesTypedSessionTests.HeartRateMeasure
                            {
                                HeartRate = i
                            }, "some-tag");
                        }

                        await session.SaveChangesAsync();
                    }
                }

                Assert.True(Server.ServerStore.DatabasesLandlord.UnloadDirectly(badName));

                var recoveryOptions = new RecoveryOptions
                {
                    PathToDataFile = badPath,
                    RecoveryDirectory = recoveredPath,
                    RecoveryTypes = RecoveryTypes.Documents | RecoveryTypes.TimeSeries
                };

                using (var store = await RecoverDatabase(recoveryOptions))
                {
                    WaitForUserToContinueTheTest(store);
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>("Users/karmel");
                        Assert.NotNull(u);
                        var ts = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>(u);
                        var entries = (await ts.GetAsync()).ToArray();
                        Assert.Equal(1000, entries.Length);
                        for (var index = 0; index < entries.Length; index++)
                        {
                            var entry = entries[index];
                            Assert.Equal(index, entry.Value.HeartRate);
                            Assert.Equal("some-tag", entry.Tag);
                        }
                    }
                }
            }
        }
    }
}
