using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_9512 : RavenTestBase
    {
        public RavenDB_9512(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);

        [Fact]
        public async Task AbortWhenNoDocsLeft()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                var actions = new List<string>();
                var sn = await store.Subscriptions.CreateAsync<User>();
                var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sn)
                {
                    CloseWhenNoDocsLeft = true,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                worker.OnSubscriptionConnectionRetry += exception =>
                {
                    actions.Add($"OnSubscriptionConnectionRetry: {exception}");
                };
                worker.OnUnexpectedSubscriptionError += exception =>
                {
                    actions.Add($"OnUnexpectedSubscriptionError: {exception}");
                };
                worker.AfterAcknowledgment += batch =>
                {
                    actions.Add($"AfterAcknowledgment ids: {string.Join(", ", batch.Items.Select(x => x.Id))}");

                    return Task.CompletedTask;
                };
                var st = worker.Run(x => { });

                Assert.True(await Assert.ThrowsAsync<SubscriptionClosedException>(() => st).WaitWithoutExceptionAsync(_reasonableWaitTime), $"Actions logs:{Environment.NewLine}"+string.Join(Environment.NewLine, actions));
            }
        }
    }
}
