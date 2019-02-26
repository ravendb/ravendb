using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Server.Config;
using Xunit;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RDBC_282 : ClusterTestBase
    {
        [Fact]
        public async Task ChangesApiShouldReconnectToServerWhenServerReturn()
        {
            var server = GetNewServer(runInMemory: false);
            try
            {
                using (var store = GetDocumentStore(new Options { Server = server }))
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

                    var nodePath = server.Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                    var url = server.WebUrl;
                    await DisposeServerAndWaitForFinishOfDisposalAsync(server);
                    var settings = new Dictionary<string, string>
                    {
                        {RavenConfiguration.GetKey(x => x.Core.ServerUrls), url}
                    };
                    server = GetNewServer(runInMemory: false, deletePrevious: false, partialPath: nodePath, customSettings: settings);

                    var url2 = server.WebUrl;
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
