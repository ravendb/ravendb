//-----------------------------------------------------------------------
// <copyright file="RevisionsTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Patch;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Revisions
{
    public class RevisionsTests : RavenTestBase
    {
        [Fact]
        public async Task CanGetRevisionsByChangeVectors()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, id);
                    await session.SaveChangesAsync();
                }
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Fitzchak " + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(11, revisionsMetadata.Count);

                    var changeVectors = revisionsMetadata.Select(x => x.GetString(Constants.Documents.Metadata.ChangeVector)).ToList();
                    changeVectors.Add("NotExistsChangeVector");

                    var revisions = await session.Advanced.Revisions.GetAsync<User>(changeVectors);
                    var first = revisions.First();
                    var last = revisions.Last();
                    Assert.NotNull(first.Value);
                    Assert.Null(last.Value);

                    Assert.NotNull(await session.Advanced.Revisions.GetAsync<User>(first.Key));
                    Assert.Null(await session.Advanced.Revisions.GetAsync<User>(last.Key));
                }
            }
        }

        [Fact]
        public async Task CanGetNullForNotExistsDocument()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.NotNull(revisions);
                    Assert.Empty(revisions);

                    var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.NotNull(metadata);
                    Assert.Empty(metadata);
                }
            }
        }

        [Fact]
        public async Task CanGetAllRevisionsFor()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    Assert.Equal("Company Name", companiesRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task CanCheckIfDocumentHasRevisions()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.Equal(DocumentFlags.HasRevisions.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task RemoveHasRevisionsFlag()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.Equal(DocumentFlags.HasRevisions.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, (config) => { config.Default.MinimumRevisionsToKeep = 0; });
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.False(metadata.TryGetValue(Constants.Documents.Metadata.Flags, out _));
                }
            }
        }

        [Fact]
        public async Task GetRevisionsOfNotExistKey()
        {

            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");
                    Assert.Equal(0, companiesRevisions.Count);
                }
            }
        }

        [Fact]
        public async Task CanExcludeEntitiesFromRevisions()
        {
            var user = new User { Name = "User Name" };
            var comment = new Comment { Name = "foo" };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.StoreAsync(comment);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Empty(await session.Advanced.Revisions.GetForAsync<Comment>(comment.Id));
                    var users = await session.Advanced.Revisions.GetForAsync<User>(user.Id);
                    Assert.Equal(1, users.Count);
                }
            }
        }

        [Fact]
        public async Task ServerSaveBundlesAfterRestart()
        {
            var path = NewDataPath();
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                var old = GetDocumentDatabaseInstanceFor(store).Result;
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    Assert.Equal("Company Name", companiesRevisions[1].Name);
                }
                var newInstance = GetDocumentDatabaseInstanceFor(store).Result;

                Assert.NotSame(old, newInstance);
            }
        }

        [Fact]
        public async Task WillCreateRevision()
        {
            var product = new User { Name = "Hibernating" };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    product.Name += " Rhinos";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    product.Name += " - RavenDB";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>(product.Id);
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Hibernating Rhinos - RavenDB", users[0].Name);
                    Assert.Equal("Hibernating Rhinos", users[1].Name);
                    Assert.Equal("Hibernating", users[2].Name);
                }
            }
        }

        [Fact]
        public async Task WillCreateValidRevisionWhenCompressionDocumentWasSaved()
        {
            var user = new User { Name = new string('1', 4096 * 2) }; // create a string which will be compressed
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var actualUser = await session.LoadAsync<User>(user.Id);
                    Assert.Equal(actualUser.Name, user.Name);

                    var users = await session.Advanced.Revisions.GetForAsync<User>(user.Id);
                    Assert.Equal(user.Name, users.Single().Name);
                }
            }
        }

        [Fact]
        public async Task WillNotCreateRevision()
        {
            var product = new Product { Description = "A fine document db", Quantity = 5 };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    product.Description = "desc 2";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    product.Description = "desc 3";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var products = await session.Advanced.Revisions.GetForAsync<Product>(product.Id);
                    Assert.Equal(0, products.Count);
                }
            }
        }

        [Fact]
        public async Task WillDeleteOldRevisions()
        {
            var company = new Company { Name = "Company #1" };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    for (var i = 0; i < 10; i++)
                    {
                        company.Name = "Company #2: " + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(5, revisions.Count);
                    Assert.Equal("Company #2: 9", revisions[0].Name);
                    Assert.Equal("Company #2: 5", revisions[4].Name);
                }
            }
        }

        [Fact]
        public async Task WillDeleteRevisionsIfDeleted_OnlyIfPurgeOnDeleteIsTrue()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Hibernating Rhinos " };
                    var user = new User { Name = "Fitzchak " };
                    await session.StoreAsync(company, "companies/1");
                    await session.StoreAsync(user, "users/1");
                    await session.SaveChangesAsync();
                }
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>("companies/1");
                        var user = await session.LoadAsync<User>("users/1");
                        company.Name += i;
                        user.Name += i;
                        await session.StoreAsync(company);
                        await session.StoreAsync(user);
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1");
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(company);
                    Assert.NotNull(user);
                    session.Delete(company);
                    session.Delete(user);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");
                    var users = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(5, companies.Count);
                    Assert.Empty(users);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "New Company" }, "companies/1");
                    await session.StoreAsync(new User { Name = "New User" }, "users/1");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");
                    var users = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(5, companies.Count);
                    Assert.Equal("New Company", companies.First().Name);
                    Assert.Equal(1, users.Count);
                }
            }
        }

        [Fact]
        public async Task RevisionsOrder()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Hibernating" }, "users/1");
                    await session.StoreAsync(new User { Name = "Hibernating11" }, "users/11");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos" }, "users/1");
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos11" }, "users/11");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos - RavenDB" }, "users/1");
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos - RavenDB11" }, "users/11");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Hibernating Rhinos - RavenDB", users[0].Name);
                    Assert.Equal("Hibernating Rhinos", users[1].Name);
                    Assert.Equal("Hibernating", users[2].Name);
                }
            }
        }

        [Fact]
        public async Task CanGetCountersSnapshotInRevisions()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    // revision 1
                    await session.StoreAsync(new Company(), "companies/1-A");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");

                    // revision 2
                    company.Name = "HR";
                    await session.SaveChangesAsync();

                    // revision 3
                    session.CountersFor(company).Increment("Likes", 100);
                    await session.SaveChangesAsync();

                    // no revision for this one
                    session.CountersFor(company).Increment("Likes", 50);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("companies/1-A");
                    Assert.Equal(3, companiesRevisions.Count);
                    var metadatas = companiesRevisions.Select(c => session.Advanced.GetMetadataFor(c)).ToList();

                    Assert.Equal("HR", companiesRevisions[0].Name);

                    var revisionCounters = (IMetadataDictionary)metadatas[0][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(1, revisionCounters.Count);
                    Assert.Equal(100L, revisionCounters["Likes"]);

                    Assert.Equal("HR", companiesRevisions[1].Name);
                    Assert.False(metadatas[1].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));

                    Assert.Null(companiesRevisions[2].Name);
                    Assert.False(metadatas[1].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));

                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");

                    // revision 4
                    company.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();

                    // revision 5
                    session.CountersFor(company).Increment("Dislikes", 20);
                    await session.SaveChangesAsync();

                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("companies/1-A");
                    Assert.Equal(5, companiesRevisions.Count);
                    var metadatas = companiesRevisions.Select(c => session.Advanced.GetMetadataFor(c)).ToList();

                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    var revisionCounters = (IMetadataDictionary)metadatas[0][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(2, revisionCounters.Count);
                    Assert.Equal(150L, revisionCounters["Likes"]);
                    Assert.Equal(20L, revisionCounters["Dislikes"]);

                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    revisionCounters = (IMetadataDictionary)metadatas[1][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(1, revisionCounters.Count);
                    Assert.Equal(150L, revisionCounters["Likes"]);

                    Assert.Equal("HR", companiesRevisions[2].Name);
                    revisionCounters = (IMetadataDictionary)metadatas[2][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(1, revisionCounters.Count);
                    Assert.Equal(100L, revisionCounters["Likes"]);

                    Assert.Equal("HR", companiesRevisions[3].Name);
                    Assert.False(metadatas[3].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));
                    
                    Assert.Null(companiesRevisions[4].Name);
                    Assert.False(metadatas[4].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));

                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task GetRevisionsBinEntries(bool useSession)
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var deletedRevisions = await store.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(0, deletedRevisions.Length);

                var id = "users/1";
                if (useSession)
                {
                    var user = new User { Name = "Fitzchak" };
                    for (var i = 0; i < 2; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(user);
                            await session.SaveChangesAsync();
                        }
                        using (var session = store.OpenAsyncSession())
                        {
                            session.Delete(user.Id);
                            await session.SaveChangesAsync();
                        }
                    }
                    id += "-A";
                }
                else
                {
                    await store.Commands().PutAsync(id, null, new User { Name = "Fitzchak" });
                    await store.Commands().DeleteAsync(id, null);
                    await store.Commands().PutAsync(id, null, new User { Name = "Fitzchak" });
                    await store.Commands().DeleteAsync(id, null);
                }

                var statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 1 : 0, statistics.CountOfDocuments);
                Assert.Equal(4, statistics.CountOfRevisionDocuments);

                deletedRevisions = await store.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(1, deletedRevisions.Length);

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.Equal(4, users.Count);
                    Assert.Equal(null, users[0].Name);
                    Assert.Equal("Fitzchak", users[1].Name);
                    Assert.Equal(null, users[2].Name);
                    Assert.Equal("Fitzchak", users[3].Name);

                    // Can get metadata only
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(4, revisionsMetadata.Count);
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), revisionsMetadata[1].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[2].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), revisionsMetadata[3].GetString(Constants.Documents.Metadata.Flags));
                }

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new AdminRevisionsHandler.Parameters
                {
                    DocumentIds = new[] { id, "users/not/exists" }
                }));

                statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 1 : 0, statistics.CountOfDocuments);
                Assert.Equal(0, statistics.CountOfRevisionDocuments);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DeleteRevisionsBeforeFromConsole(bool useConsole)
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-1);

                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Fitzchak " + i });
                        await session.SaveChangesAsync();
                    }
                }
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(1);
                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Fitzchak " + (i + 100) });
                        await session.SaveChangesAsync();
                    }
                }
                database.Time.UtcDateTime = () => DateTime.UtcNow;

                var statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(21, statistics.CountOfDocuments);
                Assert.Equal(20, statistics.CountOfRevisionDocuments);

                if (useConsole)
                {
                    new AdminJsConsole(Server, database).ApplyScript(new AdminJsScript(
                     "database.DocumentsStorage.RevisionsStorage.Operations.DeleteRevisionsBefore('Users', new Date());"));
                }
                else
                {
                    database.DocumentsStorage.RevisionsStorage.Operations.DeleteRevisionsBefore("Users", DateTime.UtcNow);
                }

                statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(21, statistics.CountOfDocuments);
                Assert.Equal(10, statistics.CountOfRevisionDocuments);
            }
        }

        public class DeleteRevisionsOperation : IMaintenanceOperation
        {
            private readonly AdminRevisionsHandler.Parameters _parameters;

            public DeleteRevisionsOperation(AdminRevisionsHandler.Parameters parameters)
            {
                _parameters = parameters;
            }

            public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new DeleteRevisionsCommand(conventions, context, _parameters);
            }

            private class DeleteRevisionsCommand : RavenCommand
            {
                private readonly BlittableJsonReaderObject _parameters;

                public DeleteRevisionsCommand(DocumentConventions conventions, JsonOperationContext context, AdminRevisionsHandler.Parameters parameters)
                {
                    if (conventions == null)
                        throw new ArgumentNullException(nameof(conventions));
                    if (context == null)
                        throw new ArgumentNullException(nameof(context));
                    if (parameters == null)
                        throw new ArgumentNullException(nameof(parameters));

                    _parameters = EntityToBlittable.ConvertCommandToBlittable(parameters, context);
                }

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/admin/revisions";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Delete,
                        Content = new BlittableJsonContent(stream =>
                        {
                            ctx.Write(stream, _parameters);
                        })
                    };
                }
            }
        }

        private class Comment
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Description { get; set; }
            public int Quantity { get; set; }
        }
    }
}
