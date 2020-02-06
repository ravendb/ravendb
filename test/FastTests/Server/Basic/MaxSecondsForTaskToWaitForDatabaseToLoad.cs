using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class MaxSecondsForTaskToWaitForDatabaseToLoad : RavenTestBase
    {
        public MaxSecondsForTaskToWaitForDatabaseToLoad(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_not_throw_when_there_no_timeout()
        {
            UseNewLocalServer();
            Server.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);

            int retries = 3;
            //in case that there is alot of stuff is going on concurrently with this test,
            //give several chances for the load to pass successfully 
            bool didPassAtLeastOnce = false;
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    using (var store = GetDocumentStore())
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

        [Fact]
        public async Task Should_throw_when_there_is_timeout()
        {
            UseNewLocalServer();
            var mre = new ManualResetEvent(false);
            Server.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(0, TimeUnit.Milliseconds);
            Server.ServerStore.DatabasesLandlord.OnDatabaseLoaded += s =>
            {
                mre.Set();
                Thread.Sleep(100);
            }; // force timeout          

            var url = Server.WebUrl;
            var name = GetDatabaseName();
            var doc = GenerateDatabaseDoc(name);

            try
            {

                var t = Task.Run(() =>
                {
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    using (var requestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, name, null, DocumentConventions.Default))
                    {
                        requestExecutor.Execute(
                            new CreateDatabaseOperation(doc).GetCommand(new DocumentConventions(), ctx), ctx);
                    }
                });

                Assert.Throws<DatabaseLoadTimeoutException>(() =>
                {
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    using (var requestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, name, null, DocumentConventions.Default))
                    {
                        mre.WaitOne();
                        requestExecutor.Execute(new GetDocumentsCommand("Raven/HiloPrefix", includes: null, metadataOnly: false), ctx);
                    }
                });
                await t;
            }
            catch (TaskCanceledException e)
            {
                Console.WriteLine(e.InnerException);
                throw;
            }
        }

        private static DatabaseRecord GenerateDatabaseDoc(string name)
        {
            var doc = new DatabaseRecord(name);
            doc.Settings[RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1";
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "true";
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true";
            doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString();

            return doc;
        }
    }
}
