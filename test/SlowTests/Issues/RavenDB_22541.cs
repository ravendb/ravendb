using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Sparrow.Extensions;
using Raven.Server.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_22541 : RavenTestBase
    {
        public RavenDB_22541(ITestOutputHelper output) : base(output)
        {
        }

        public static IEnumerable<object[]> Schedules => new[]
        {
            new object[] { Constants.Documents.Metadata.Expires },
            new object[] { Constants.Documents.Metadata.Refresh },
            new object[] { Constants.Documents.Metadata.ArchiveAt }
        };

        [RavenTheory(RavenTestCategory.Revisions)]
        [MemberData(nameof(Schedules))]
        public async Task RevertingToARevisionStripsTheSchedule(string schedule)
        {
            const string id = "companies/1";

            using var store = GetDocumentStore();
            await RevisionsHelper.SetupRevisionsAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var company = new Company { Name = "Scheduled" };
                await session.StoreAsync(company, id);
                session.Advanced.GetMetadataFor(company)[schedule] = DateTime.UtcNow.AddDays(7).GetDefaultRavenFormat(isUtc: true);
                await session.SaveChangesAsync();
            }

            var revertTo = DateTime.UtcNow;

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>(id);
                company.Name = "Changed";
                await session.SaveChangesAsync();
            }

            var operation = await store.Maintenance.SendAsync(new RevertRevisionsOperation(revertTo, 60));
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

            using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var company = await session.LoadAsync<Company>(id);

                Assert.Equal("Scheduled", company.Name);
                Assert.False(session.Advanced.GetMetadataFor(company).ContainsKey(schedule));
            }
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        [MemberData(nameof(Schedules))]
        public async Task RevertingASingleDocumentStripsTheSchedule(string schedule)
        {
            const string id = "companies/1";

            using var store = GetDocumentStore();
            await RevisionsHelper.SetupRevisionsAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var company = new Company { Name = "Scheduled" };
                await session.StoreAsync(company, id);
                session.Advanced.GetMetadataFor(company)[schedule] = DateTime.UtcNow.AddDays(7).GetDefaultRavenFormat(isUtc: true);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>(id);
                company.Name = "Changed";
                await session.SaveChangesAsync();
            }

            string changeVector;
            using (var session = store.OpenAsyncSession())
            {
                var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                changeVector = metadata.Last().GetString(Constants.Documents.Metadata.ChangeVector);
            }

            await store.Operations.SendAsync(new RevertRevisionsByIdOperation(new Dictionary<string, string> { [id] = changeVector }));

            using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var company = await session.LoadAsync<Company>(id);

                Assert.Equal("Scheduled", company.Name);
                Assert.False(session.Advanced.GetMetadataFor(company).ContainsKey(schedule));
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevertingKeepsTheRestOfTheMetadata()
        {
            const string id = "companies/1";

            using var store = GetDocumentStore();
            await RevisionsHelper.SetupRevisionsAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var company = new Company { Name = "Scheduled" };
                await session.StoreAsync(company, id);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddDays(7).GetDefaultRavenFormat(isUtc: true);
                metadata["Origin"] = "import";
                await session.SaveChangesAsync();
            }

            var revertTo = DateTime.UtcNow;

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>(id);
                company.Name = "Changed";
                await session.SaveChangesAsync();
            }

            var operation = await store.Maintenance.SendAsync(new RevertRevisionsOperation(revertTo, 60));
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

            using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var company = await session.LoadAsync<Company>(id);
                var metadata = session.Advanced.GetMetadataFor(company);

                Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Expires));
                Assert.Equal("import", metadata.GetString("Origin"));
            }
        }
    }
}
