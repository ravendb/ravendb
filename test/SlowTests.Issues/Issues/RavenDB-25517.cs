using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25517 : RavenTestBase
{

    public RavenDB_25517(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Subscriber_doesnt_get_disposed_on_other_subscriber_dispose(bool same)
    {
        using (var store = GetDocumentStore())
        {
            // Create initial document
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestDocument { TimeStamp = DateTime.Now.ToString() }, "Test/1-A");
                await session.SaveChangesAsync();
            }

            var messages_v1 = new List<string>();
            var messages_v2 = new List<string>();

            var changes = store.Changes();
            await changes.EnsureConnectedNow();

            IChangesObservable<DocumentChange> documentChanges1;
            IChangesObservable<DocumentChange> documentChanges2;
            if (same)
            {
                documentChanges1 = documentChanges2 = changes.ForDocument("Test/1-A");
            }
            else
            {
                documentChanges1 = changes.ForDocument("Test/1-A");
                documentChanges2 = changes.ForDocument("Test/1-A");
            }

            // Subscribe twice to the same document

            var subscription_v1 = documentChanges1.Subscribe(change =>
            {
                if (change.Type == DocumentChangeTypes.Delete)
                {
                    messages_v2.Add($"[V1] Change '{change.Type}' detected for document ID: {change.Id}");
                    return;
                }

                messages_v1.Add($"[V1] Change '{change.Type}' detected for document ID: {change.Id} at {DateTime.Now}");
            });

            var subscription_v2 = documentChanges2.Subscribe(change =>
            {
                if (change.Type == DocumentChangeTypes.Delete)
                {
                    messages_v2.Add($"[V2] Change '{change.Type}' detected for document ID: {change.Id}");
                    return;
                }

                messages_v2.Add($"[V2] Change '{change.Type}' detected for document ID: {change.Id} at {DateTime.Now}");
            });

            await documentChanges1.EnsureSubscribedNow();
            await documentChanges2.EnsureSubscribedNow();

            // First change - both subscriptions should receive notification
            using (var session = store.OpenAsyncSession())
            {
                var entity = new TestDocument { TimeStamp = DateTime.Now.ToString() };
                await session.StoreAsync(entity, "Test/1-A");
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForValueAsync(() => messages_v1.Any(), true));
            Assert.True(await WaitForValueAsync(() => messages_v2.Any(), true));

            // Dispose first subscription
            subscription_v1.Dispose();
            // Dispose first subscription again
            subscription_v1.Dispose();

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Test/1-A");
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForValueAsync(() => messages_v2.Count >= 2 && messages_v2.Contains("[V2] Change 'Delete' detected for document ID: Test/1-A"), true));

            Assert.NotEqual(messages_v1.Count, messages_v2.Count);

            Assert.Contains("[V2] Change 'Delete' detected for document ID: Test/1-A", messages_v2);
        }
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Each_subscriber_receives_every_notification_twice(bool same)
    {
        using (var store = GetDocumentStore())
        {
            // Create initial document
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestDocument { TimeStamp = DateTime.Now.ToString() }, "Test/1-A");
                await session.SaveChangesAsync();
            }

            var messages_v1 = new List<string>();
            var messages_v2 = new List<string>();

            var changes = store.Changes();
            await changes.EnsureConnectedNow();

            IChangesObservable<DocumentChange> documentChanges1;
            IChangesObservable<DocumentChange> documentChanges2;
            if (same)
            {
                documentChanges1 = documentChanges2 = changes.ForDocument("Test/1-A");
            }
            else
            {
                documentChanges1 = changes.ForDocument("Test/1-A");
                documentChanges2 = changes.ForDocument("Test/1-A");
            }

            // Subscribe twice to the same document

            var subscription_v1 = documentChanges1.Subscribe(change =>
            {
                messages_v1.Add($"[V1] Change '{change.Type}' detected for document ID: {change.Id} at {DateTime.Now}");
            });

            var subscription_v2 = documentChanges2.Subscribe(change =>
            {
                messages_v2.Add($"[V2] Change '{change.Type}' detected for document ID: {change.Id} at {DateTime.Now}");
            });

            await documentChanges1.EnsureSubscribedNow();
            await documentChanges2.EnsureSubscribedNow();

            // First change - both subscriptions should receive notification
            using (var session = store.OpenAsyncSession())
            {
                var entity = new TestDocument { TimeStamp = DateTime.Now.ToString() };
                await session.StoreAsync(entity, "Test/1-A");
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForValueAsync(() => messages_v1.Any(), true));
            Assert.True(await WaitForValueAsync(() => messages_v2.Any(), true));

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Test/1-A");
                await session.SaveChangesAsync();
            }

            Assert.True(await WaitForValueAsync(() => messages_v1.Count >= 2, true));
            Assert.True(await WaitForValueAsync(() => messages_v2.Count >= 2, true));

            var l = messages_v1.GroupBy(x => x).Select(y => new { Key = y.Key, Count = y.Count() }).ToList();
            Assert.All(l, x => Assert.Equal(1, x.Count));
        }
    }

    private class TestDocument
    {
        public string Id { get; set; }

        public string TimeStamp { get; set; }
    }
}
