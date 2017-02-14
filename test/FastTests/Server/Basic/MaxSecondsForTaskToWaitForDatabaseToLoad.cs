using System;
using System.Threading;
using Raven.Client.Exceptions.Database;
using Raven.Server;
using Raven.Server.Config.Settings;
using Xunit;

namespace FastTests.Server.Basic
{
    public class MaxSecondsForTaskToWaitForDatabaseToLoad : RavenNewTestBase
    {
      
        [Fact]
        public void ShouldThrow_DatabaseLoadTimeout()
        {
            using (var server = GetNewServer(modifyConfig: config =>
            {
                config.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(0, TimeUnit.Milliseconds);
            }))
            {
                server.ServerStore.DatabasesLandlord.OnDatabaseLoaded += s => Thread.Sleep(100);
                    // force timeout           

                Assert.Throws<DatabaseLoadTimeoutException>(() =>
                {
                    using (var store = GetDocumentStore(server))
                    using (var session = store.OpenSession())
                    {
                        session.Load<object>("Raven/ServerPrefixForHilo");
                    }
                });
            }

            using (var server = GetNewServer(modifyConfig: config =>
            {
                config.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);
            }))
            {

                int retries = 3;
                //in case that there is alot of stuff is going on concurrently with this test,
                //give several chances for the load to pass successfully 
                bool didPassAtLeastOnce = false;
                for (int i = 0; i < 3; i++)
                {
                    try
                    {
                        using (var store = GetDocumentStore(server))
                        using (var session = store.OpenSession())
                        {
                            session.Load<object>("Raven/ServerPrefixForHilo");
                        }
                        didPassAtLeastOnce = true;
                        break;
                    }
                    catch (DatabaseLoadTimeoutException)
                    {
                        if (--retries == 0)
                            throw;
                    }
                }

                Assert.True(didPassAtLeastOnce);
            }
        }
    }
}
