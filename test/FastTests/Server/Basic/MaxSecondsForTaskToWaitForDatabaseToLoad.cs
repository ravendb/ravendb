using System;
using System.Threading;
using Raven.NewClient.Client.Exceptions.Database;
using Raven.Server.Config.Settings;
using Xunit;

namespace FastTests.Server.Basic
{
    public class MaxSecondsForTaskToWaitForDatabaseToLoad : RavenNewTestBase
    {
        [Fact(Skip = "Temporary skip while working on RavenDB-6219. After finishing it, this test should be investigated!")]
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

            tryLoad.Invoke();
        }
    }
}
