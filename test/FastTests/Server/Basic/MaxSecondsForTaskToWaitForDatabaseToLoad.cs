using System;
using System.Threading;
using Raven.Client.Exceptions.Database;
using Raven.Server.Config.Settings;
using Xunit;

namespace FastTests.Server.Basic
{
    public class MaxSecondsForTaskToWaitForDatabaseToLoad : RavenTestBase
    {
        [Fact]
        public void ShouldThrow_DatabaseLoadTimeout()
        {
            DoNotReuseServer();
            Server.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(0, TimeUnit.Milliseconds);
            Server.ServerStore.DatabasesLandlord.OnDatabaseLoaded += s => Thread.Sleep(100);// force timeout
            var tryLoad = new Action(delegate
            {
                using (var store = GetDocumentStore())
                using (var session = store.OpenSession())
                {
                    session.Load<object>("Raven/ServerPrefixForHilo");
                }
            });

            Assert.Throws<DatabaseLoadTimeoutException>(tryLoad);

            Server.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);

            int retries = 3;
            //in case that there is alot of stuff is going on concurrently with this test,
            //give several chances for the load to pass successfully 
            bool didPassAtLeastOnce = false;
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    tryLoad.Invoke();
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
