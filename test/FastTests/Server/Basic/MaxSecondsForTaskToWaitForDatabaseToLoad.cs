using System;
using Raven.Abstractions.Connection;
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
            Server.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(1, TimeUnit.Milliseconds);

            var tryLoad = new Action((delegate
            {
                using (var store = GetDocumentStore())
                using (var session = store.OpenSession())
                {
                    session.Load<object>("Raven/ServerPrefixForHilo");
                }
            }));

            Assert.Throws<ErrorResponseException>(tryLoad);

            Server.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);

            tryLoad.Invoke();
        }
    }
}
