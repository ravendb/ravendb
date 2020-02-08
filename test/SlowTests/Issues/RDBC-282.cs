using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_282 : ClusterTestBase
    {
        public RDBC_282(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ChangesApiShouldReconnectToServerWhenServerReturn()
        {
            var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, RegisterForDisposal = false });
            try
            {
                using (var store = GetDocumentStore(new Options { Server = server, Path = Path.Combine(server.Configuration.Core.DataDirectory.FullPath, "ChangesApiShouldReconnectToServerWhenServerReturn") }))
                {
                    var list = new BlockingCollection<DocumentChange>();
                    var taskObservable = store.Changes();
                    await taskObservable.EnsureConnectedNow();
                    var observableWithTask = taskObservable.ForDocumentsInCollection("Users");
                    observableWithTask.Subscribe(list.Add);
                    await observableWithTask.EnsureSubscribedNow();

                    PushUser(store);

                    var value = WaitForValue(() => list.Count, 1);
                    Assert.Equal(1, value);

                    var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

                    var settings = new Dictionary<string, string>
                    {
                        {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url}
                    };
                    server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings });
                    await taskObservable.EnsureConnectedNow();
                    PushUser(store);
                    value = WaitForValue(() => list.Count, 2);
                    Assert.Equal(2, value);
                }
            }
            finally
            {
                server?.Dispose();
            }
        }

        private static void PushUser(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/");
                session.SaveChanges();
            }
        }
    }
}
