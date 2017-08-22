//-----------------------------------------------------------------------
// <copyright file="RevisionsTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Revisions
{
    public class RevisionsTests : RavenTestBase
    {
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
                    var companiesRevisions = await session.Advanced.GetRevisionsForAsync<Company>(company.Id);
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
        public async Task GetRevisionsOfNotExistKey()
        {

            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.GetRevisionsForAsync<Company>("companies/1");
                    Assert.Equal(0, companiesRevisions.Count);
                }
            }
        }

        [Fact]
        public async Task GetRevisionsOfNotExistKey_WithRevisionsDisabled()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var exception = await Assert.ThrowsAsync<RevisionsDisabledException>(async () => await session.Advanced.GetRevisionsForAsync<Company>("companies/1"));
                    Assert.Contains("Revisions are disabled", exception.Message);
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
                    Assert.Empty(await session.Advanced.GetRevisionsForAsync<Comment>(comment.Id));
                    var users = await session.Advanced.GetRevisionsForAsync<User>(user.Id);
                    Assert.Equal(1, users.Count);
                }
            }
        }

        [Fact]
        public async Task ServerSaveBundlesAfterRestart()
        {
            var path = NewDataPath();
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore(path: path))
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
                Server.ServerStore.DatabasesLandlord.UnloadDatabase(store.Database, null, db => false);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.GetRevisionsForAsync<Company>(company.Id);
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
                    var users = await session.Advanced.GetRevisionsForAsync<User>(product.Id);
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Hibernating Rhinos - RavenDB", users[0].Name);
                    Assert.Equal("Hibernating Rhinos", users[1].Name);
                    Assert.Equal("Hibernating", users[2].Name);
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
                    var products = await session.Advanced.GetRevisionsForAsync<Product>(product.Id);
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
                    var revisions = await session.Advanced.GetRevisionsForAsync<Company>(company.Id);
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
                    var companies = await session.Advanced.GetRevisionsForAsync<Company>("companies/1");
                    var users = await session.Advanced.GetRevisionsForAsync<User>("users/1");
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
                    var companies = await session.Advanced.GetRevisionsForAsync<Company>("companies/1");
                    var users = await session.Advanced.GetRevisionsForAsync<User>("users/1");
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
                    var users = await session.Advanced.GetRevisionsForAsync<User>("users/1");
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Hibernating Rhinos - RavenDB", users[0].Name);
                    Assert.Equal("Hibernating Rhinos", users[1].Name);
                    Assert.Equal("Hibernating", users[2].Name);
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
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, false);

                var deletedRevisions = await store.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(0, deletedRevisions.Count);

                var id = "users/1";
                if (useSession)
                {
                    var user = new User {Name = "Fitzchak"};
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
                    await store.Commands().PutAsync(id, null, new User {Name = "Fitzchak"});
                    await store.Commands().DeleteAsync(id, null);
                    await store.Commands().PutAsync(id, null, new User {Name = "Fitzchak"});
                    await store.Commands().DeleteAsync(id, null);
                }

                var statistics = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 1 : 0, statistics.CountOfDocuments);
                Assert.Equal(4, statistics.CountOfRevisionDocuments);

                deletedRevisions = await store.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(1, deletedRevisions.Count);

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced.GetRevisionsForAsync<User>(id);
                    Assert.Equal(4, users.Count);
                    Assert.Equal(null, users[0].Name);
                    Assert.Equal("Fitzchak", users[1].Name);
                    Assert.Equal(null, users[2].Name);
                    Assert.Equal("Fitzchak", users[3].Name);
                }

                // Can get metadata only
                dynamic revisions = await store.Commands().GetRevisionsForAsync(id, metadataOnly: true);
                Assert.Equal(4, revisions.Count);
                Assert.Equal(DocumentFlags.DeleteRevision.ToString(), revisions[0][Constants.Documents.Metadata.Key][Constants.Documents.Metadata.Flags]);
                Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), revisions[1][Constants.Documents.Metadata.Key][Constants.Documents.Metadata.Flags]);
                Assert.Equal(DocumentFlags.DeleteRevision.ToString(), revisions[2][Constants.Documents.Metadata.Key][Constants.Documents.Metadata.Flags]);
                Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), revisions[3][Constants.Documents.Metadata.Key][Constants.Documents.Metadata.Flags]);

                await store.Admin.SendAsync(new DeleteRevisionsOperation(id, "users/not/exists"));

                statistics = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 1 : 0, statistics.CountOfDocuments);
                Assert.Equal(0, statistics.CountOfRevisionDocuments);
            }
        }

        [Theory(Skip="RavenDB-8265")]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DeleteRevisionsBeforeFromConsole(bool useConsole)
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, false);

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-1);

                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User {Name = "Fitzchak " + i});
                        await session.SaveChangesAsync();
                    }
                }
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(1);
                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User {Name = "Fitzchak " + (i + 100)});
                        await session.SaveChangesAsync();
                    }
                }
                database.Time.UtcDateTime = () => DateTime.UtcNow;

                var statistics = store.Admin.Send(new GetStatisticsOperation());
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

                statistics = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(21, statistics.CountOfDocuments);
                Assert.Equal(10, statistics.CountOfRevisionDocuments);
            }
        }

        public class DeleteRevisionsOperation : IAdminOperation
        {
            private readonly string[] _ids;

            public DeleteRevisionsOperation(params string[] ids)
            {
                _ids = ids;
            }

            public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new DeleteRevisionsCommand(_ids);
            }

            private class DeleteRevisionsCommand : RavenCommand
            {
                private readonly string[] _ids;

                public DeleteRevisionsCommand(params string[] ids)
                {
                    _ids = ids;
                }

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/admin/revisions?");

                    foreach (var id in _ids)
                    {
                        sb.Append("&id=");
                        sb.Append(id);
                    }

                    url = sb.ToString();

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Delete,
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
