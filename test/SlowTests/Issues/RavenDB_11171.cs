using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11171 : RavenTestBase
    {
        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    await session.StoreAsync(company, "companies/1");

                    var stream = new MemoryStream(Encoding.UTF8.GetBytes("123"));
                    session.Advanced.Attachments.Store(company, "photo.jpg", stream);

                    await session.SaveChangesAsync();
                }

                await AssertAttachment(store);

                using (var store2 = GetDocumentStore())
                {
                    await RevisionsHelper.SetupRevisions(store2, Server.ServerStore, new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration()
                    });

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), store2.Smuggler);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    await AssertAttachment(store2);

                    await AssertRevision(store2, 1);

                    using (var store3 = GetDocumentStore())
                    {
                        await RevisionsHelper.SetupRevisions(store3, Server.ServerStore, new RevisionsConfiguration
                        {
                            Default = new RevisionsCollectionConfiguration()
                        });

                        operation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), store3.Smuggler);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await AssertAttachment(store3);

                        await AssertRevision(store3, 2);
                    }
                }
            }
        }

        private static async Task AssertRevision(IDocumentStore store, int expectedNumberOfRevisions)
        {
            using (var session = store.OpenAsyncSession())
            {
                var revisions = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");

                Assert.NotNull(revisions);
                Assert.Equal(expectedNumberOfRevisions, revisions.Count);

                foreach (var revision in revisions)
                {
                    var attachmentNames = session.Advanced.Attachments.GetNames(revision);
                    Assert.NotNull(attachmentNames);
                    Assert.Equal(1, attachmentNames.Length);

                    foreach (var attachmentName in attachmentNames)
                    {
                        var attachment = await session.Advanced.Attachments.GetRevisionAsync(revision.Id, attachmentName.Name, session.Advanced.GetChangeVectorFor(revision));
                        Assert.NotNull(attachment);
                        Assert.NotNull(attachment.Stream);

                        using (var sr = new StreamReader(attachment.Stream))
                        {
                            var value = sr.ReadToEnd();
                            Assert.Equal("123", value);
                        }
                    }
                }
            }
        }

        private static async Task AssertAttachment(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>("companies/1");
                Assert.NotNull(company);

                var attachmentNames = session.Advanced.Attachments.GetNames(company);
                Assert.NotNull(attachmentNames);
                Assert.Equal(1, attachmentNames.Length);

                foreach (var attachmentName in attachmentNames)
                {
                    var attachment = await session.Advanced.Attachments.GetAsync(company, attachmentName.Name);
                    Assert.NotNull(attachment);
                    Assert.NotNull(attachment.Stream);

                    using (var sr = new StreamReader(attachment.Stream))
                    {
                        var value = sr.ReadToEnd();
                        Assert.Equal("123", value);
                    }
                }
            }
        }
    }
}
