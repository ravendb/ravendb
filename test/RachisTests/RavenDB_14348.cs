using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class RavenDB_14348 : RavenTestBase
    {
        public RavenDB_14348(ITestOutputHelper output) : base(output)
        {
        }

        // https://github.com/ravendb/ravendb/pull/9996#discussion_r361162789
        [Fact]
        public async Task CanDisposeAndDeleteWhileInsertingDocuments()
        {
            DoNotReuseServer();
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var store = GetDocumentStore())
            {
                _ = Task.Run(async () =>
                {
                    while (false == store.WasDisposed)
                    {
                        cts.Token.ThrowIfCancellationRequested();

                        await ContinuouslyGenerateDocsInternal(10, store, cts.Token);
                    }
                }, cts.Token);

                await Task.Delay(5555, cts.Token);
            }
        }

        private static async Task ContinuouslyGenerateDocsInternal(int DocsBatchSize, DocumentStore store, CancellationToken token)
        {
            try
            {
                var ids = new List<string>();
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        User entity = new User { Name = "ClusteredJohnny" + k };
                        await session.StoreAsync(entity, token);
                        ids.Add(session.Advanced.GetDocumentId(entity));
                    }

                    await session.SaveChangesAsync(token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        await session.StoreAsync(new User { Name = "Johnny" + k }, token);
                    }

                    await session.SaveChangesAsync(token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var k = 0; k < DocsBatchSize; k++)
                    {
                        var user = await session.LoadAsync<User>(ids[k], token);
                        user.Age++;
                    }

                    await session.SaveChangesAsync(token);
                }

                await Task.Delay(16, token);
            }
            catch (AllTopologyNodesDownException)
            {
            }
            catch (DatabaseDisabledException)
            {
            }
            catch (DatabaseDoesNotExistException)
            {
            }
            catch (RavenException)
            {
            }
        }
    }
}
