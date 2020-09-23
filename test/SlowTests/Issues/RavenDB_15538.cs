using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Graph;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15538 : ReplicationTestBase
    {
        public RavenDB_15538(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task GetAttachmentsForDocumentShouldReturnAttachmentsOfRevisions()
        {
            using (var store = GetDocumentStore())
            {
                await SetupRevisionsForTest(Server, store, store.Database);
                var id = "user/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR_1" }, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    using var stream = new MemoryStream(new byte[] { 3, 2, 2 });
                    session.Advanced.Attachments.Store(id, 1.ToString(), stream, "image/png");
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(id);
                    u.Name = "RGE" + u.Name.Substring(3);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(id);
                    u.Age = 30;
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var m = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(4, m.Count);
                    var cv = (string)m.First()[Constants.Documents.Metadata.ChangeVector];
                    var db = await GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                    using (documentsContext.OpenReadTransaction())
                    {
                        var lzv = documentsContext.GetLazyString(id.ToLowerInvariant());
                        var attachments = db.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(documentsContext, AttachmentType.Revision, lzv, cv).ToList();
                        Assert.Equal(1, attachments.Count);
                        var revisionsAttachment = attachments.First();
                        Assert.Equal("bucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=", revisionsAttachment.Base64Hash.ToString());
                        Assert.StartsWith("A:10", revisionsAttachment.ChangeVector);
                        Assert.StartsWith($"user/1\u001er\u001e{revisionsAttachment.ChangeVector}\u001e1\u001e{revisionsAttachment.Base64Hash}\u001eimage/png", revisionsAttachment.Key);
                    }
                }
            }
        }

        private async Task SetupRevisionsForTest(RavenServer server, DocumentStore store, string database)
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = int.MaxValue },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = int.MaxValue }
                }
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
                var (index, _) = await server.ServerStore.ModifyDatabaseRevisions(context, store.Database, configurationJson, Guid.NewGuid().ToString());
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, server.ServerStore.Engine.OperationTimeout);
            }
        }
    }
}
