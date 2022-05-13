using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Changes;

public class BasicChangesDocumentsTests : RavenTestBase
{
    public BasicChangesDocumentsTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Subscribe_To_Single_Document_Changes(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var changes = store.Changes())
            {
                await changes.EnsureConnectedNow();

                const int numberOfDocuments = 10;

                var cde = new CountdownEvent(numberOfDocuments);

                for (var i = 0; i < numberOfDocuments; i++)
                {
                    var forDocument = changes
                        .ForDocument($"orders/{i}");

                    forDocument.Subscribe(x => cde.Signal());

                    await forDocument.EnsureSubscribedNow();
                }

                for (var i = 0; i < numberOfDocuments; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order(), $"orders/{i}");
                        await session.SaveChangesAsync();
                    }
                }

                Assert.True(cde.Wait(TimeSpan.FromSeconds(60)), $"Missed {cde.CurrentCount} events.");
            }
        }
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Subscribe_To_All_Document_Changes(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var changes = store.Changes())
            {
                await changes.EnsureConnectedNow();

                const int numberOfDocuments = 10;

                var cde = new CountdownEvent(numberOfDocuments);

                var forDocuments = changes
                    .ForAllDocuments();

                forDocuments.Subscribe(x => cde.Signal());

                await forDocuments.EnsureSubscribedNow();

                for (var i = 0; i < numberOfDocuments; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order(), $"orders/{i}");
                        await session.SaveChangesAsync();
                    }
                }

                Assert.True(cde.Wait(TimeSpan.FromSeconds(60)), $"Missed {cde.CurrentCount} events.");
            }
        }
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Subscribe_To_Collection_Document_Changes(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var changes = store.Changes())
            {
                await changes.EnsureConnectedNow();

                const int numberOfDocuments = 10;

                var cde = new CountdownEvent(numberOfDocuments);

                var forDocuments = changes
                    .ForDocumentsInCollection("Orders");

                forDocuments.Subscribe(x => cde.Signal());

                await forDocuments.EnsureSubscribedNow();

                for (var i = 0; i < numberOfDocuments; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order(), $"orders/{i}");
                        await session.StoreAsync(new Company(), $"companies/{i}");
                        await session.SaveChangesAsync();
                    }
                }

                Assert.True(cde.Wait(TimeSpan.FromSeconds(60)), $"Missed {cde.CurrentCount} events.");
            }
        }
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Subscribe_To_Prefix_Document_Changes(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var changes = store.Changes())
            {
                await changes.EnsureConnectedNow();

                const int numberOfDocuments = 12;

                var cde = new CountdownEvent(3);

                var forDocuments = changes
                    .ForDocumentsStartingWith("orders/1");

                forDocuments.Subscribe(x => cde.Signal());

                await forDocuments.EnsureSubscribedNow();

                for (var i = 0; i < numberOfDocuments; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order(), $"orders/{i}");
                        await session.StoreAsync(new Company(), $"companies/{i}");
                        await session.SaveChangesAsync();
                    }
                }

                Assert.True(cde.Wait(TimeSpan.FromSeconds(10)), $"Missed {cde.CurrentCount} events.");
            }
        }
    }
}
