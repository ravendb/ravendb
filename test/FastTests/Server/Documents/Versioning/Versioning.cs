//-----------------------------------------------------------------------
// <copyright file="Versioning.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Xunit;
using Raven.Client.Bundles.Versioning;

namespace FastTests.Server.Documents.Versioning
{
    public class Versioning : VersioningTest
    {
        [Fact]
        public async Task CanGetAllRevisionsFor()
        {
            var company = new Company {Name = "Company Name"};
            using (var store = await GetDocumentStore())
            {
                await SetupVersioning(store);
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
                    Assert.Equal(2, companiesRevisions.Length);
                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task GetRevisionsOfNotExistKey()
        {
            using (var store = await GetDocumentStore())
            {
                await SetupVersioning(store);
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.GetRevisionsForAsync<Company>("companies/1");
                    Assert.Equal(0, companiesRevisions.Length);
                }
            }
        }

        [Fact]
        public async Task GetRevisionsOfNotExistKey_WithVersioningDisabled()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var exception = await Assert.ThrowsAsync<ErrorResponseException>(async () => await session.Advanced.GetRevisionsForAsync<Company>("companies/1"));
                    Assert.Contains("Versioning is disabled", exception.Message);
                }
            }
        }

        [Fact]
        public async Task CanExcludeEntitiesFromVersioning()
        {
            var user = new User { Name = "User Name" };
            var comment = new Comment { Name = "foo" };
            using (var store = await GetDocumentStore())
            {
                await SetupVersioning(store);
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
                    Assert.Equal(1, users.Length);
                }
            }
        }

        [Fact]
        public async Task WillCreateRevisionIfExplicitlyRequested()
        {
            var product = new Product {Description = "A fine document db", Quantity = 5};
            using (var store = await GetDocumentStore())
            {
                await SetupVersioning(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    product.Description = "desc 2";
                    await session.StoreAsync(product);
                    session.Advanced.ExplicitlyVersion(product);
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
                    Assert.Equal("desc 2", products.Single().Description);
                }
            }
        }


        [Fact]
        public async Task WillDeleteOldRevisions()
        {
            var company = new Company {Name = "Company #1"};
            using (var store = await GetDocumentStore())
            {
                await SetupVersioning(store);
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
                    Assert.Equal(5, revisions.Length);
                    Assert.Equal("Company #2: 5", revisions[0].Name);
                    Assert.Equal("Company #2: 9", revisions[4].Name);
                }
            }
        }

        /*         [Fact]
            public async Task Will_not_delete_revisions_if_parent_exists()
            {
                var company = new Company { Name = "Company Name" };
                using (var store = await GetDocumentStore())
                {
                    await SetupVersioning(store);
                    using (var session = store.OpenAsyncSession())
                    {
                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1/revisions/1");
                    Assert.NotNull(doc);

                    session.Advanced.Defer(new DeleteCommandData
                    {
                        Key = "companies/1/revisions/1",
                        TransactionInformation = new TransactionInformation()
                    });

                    Assert.Throws<ErrorResponseException>(() => session.SaveChanges());
                }
            }

            [Fact]
            public async Task Will_delete_revisions_if_version_is_deleted()
            {
                var company = new Company { Name = "Company Name" };
                using (var store = await GetDocumentStore())
                {
                    await SetupVersioning(store);
                    using (var session = store.OpenAsyncSession())
                    {
                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1/revisions/1");
                    var comp = session.Load<object>("companies/1");
                    Assert.NotNull(doc);

                    session.Delete(comp);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1/revisions/1");
                    Assert.NotNull(doc);

                    session.Advanced.Defer(new DeleteCommandData
                    {
                        Key = "companies/1/revisions/1",
                        TransactionInformation = new TransactionInformation()
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1/revisions/1");
                    Assert.Null(doc);
                }
            }

            [Fact]
            public async Task Will_delete_child_revisions_if_purge_is_true()
            {
                using (var store = await GetDocumentStore())
                {
                    await SetupVersioning(store);
                    using (var session = store.OpenAsyncSession())
                    {
                    session.Store(new VersioningConfiguration
                    {
                        Exclude = false,
                        PurgeOnDelete = true,
                        Id = "Raven/Versioning/Companies"
                    });

                    session.SaveChanges();
                }

                var company = new Company { Name = "Company Name" };
                using (var session = store.OpenAsyncSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1");
                    Assert.NotNull(doc);

                    session.Delete(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1/revisions/1");
                    Assert.Null(doc);
                }
            }

            [Fact]
            public async Task Will_not_delete_child_revisions_if_purge_is_false()
            {
                using (var store = await GetDocumentStore())
                {
                    await SetupVersioning(store);
                    using (var session = store.OpenAsyncSession())
                    {
                    session.Store(new VersioningConfiguration
                    {
                        Exclude = false,
                        PurgeOnDelete = false,
                        Id = "Raven/Versioning/Companies"
                    });

                    session.SaveChanges();
                }

                var company = new Company { Name = "Company Name" };
                using (var session = store.OpenAsyncSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1");
                    Assert.NotNull(doc);

                    session.Delete(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<object>("companies/1/revisions/1");
                    Assert.NotNull(doc);
                }
            }

            [Fact]
            public async Task After_a_put_delete_put_sequence_Will_continue_revision_numbers_from_last_value_if_purge_is_false()
            {
                using (var store = await GetDocumentStore())
                {
                    await SetupVersioning(store);
                    using (var session = store.OpenAsyncSession())
                    {
                    session.Store(new VersioningConfiguration
                    {
                        Exclude = false,
                        PurgeOnDelete = false,
                        Id = "Raven/Versioning/Companies",
                        MaxRevisions = 5
                    });
                    session.SaveChanges();
                }

                var company = new Company { Id = "companies/1", Name = "Company Name" };

                using (var session = store.OpenAsyncSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                    company.Name = "Company Name 2";
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<Company>("companies/1");
                    var metadata = session.Advanced.GetMetadataFor(doc);
                    Assert.Equal(2, metadata.Value<int>("Raven-Document-Revision"));

                    session.Delete(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Store(company);
                    session.SaveChanges();

                    var metadata = session.Advanced.GetMetadataFor(company);
                    Assert.Equal(3, metadata.Value<int>("Raven-Document-Revision"));
                }
            }

            [Fact, Trait("Category", "Smuggler")]
            public async Task Previously_deleted_docs_will_survive_export_import_cycle_if_purge_is_false()
            {
                using (var store = await GetDocumentStore())
                {
                    await SetupVersioning(store);
                    using (var session = store.OpenAsyncSession())
                    {
                    session.Store(new VersioningConfiguration
                    {
                        Exclude = false,
                        PurgeOnDelete = false,
                        Id = "Raven/Versioning/Companies",
                        MaxRevisions = 5
                    });
                    session.SaveChanges();
                }

                var company = new Company { Id = "companies/1", Name = "Company Name" };

                using (var session = store.OpenAsyncSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                    company.Name = "Company Name 2";
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = session.Load<Company>("companies/1");
                    Assert.Equal(2, session.Advanced.GetMetadataFor(doc).Value<int>("Raven-Document-Revision"));

                    session.Delete(doc);
                    session.SaveChanges();
                }

                var file = Path.GetTempFileName();
                try
                {
                    new SmugglerDatabaseApi().ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = file, From = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase } }).Wait();

                    using (var documentStore2 = CreateDocumentStore(port: 8078))
                    {
                        var importSmuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions()
                        {
                            ShouldDisableVersioningBundle = true
                        });
                        importSmuggler.ImportData(
                            new SmugglerImportOptions<RavenConnectionStringOptions>
                            {
                                FromFile = file,
                                To = new RavenConnectionStringOptions
                                {
                                    Url = documentStore2.Url,
                                    Credentials = documentStore2.Credentials,
                                    DefaultDatabase = documentStore2.DefaultDatabase
                                }
                            }).Wait();

                        using (var session = documentStore2.OpenAsyncSession())
                        {
                            session.Store(company);
                            session.SaveChanges();
                            Assert.Equal(3, session.Advanced.GetMetadataFor(company).Value<int>("Raven-Document-Revision"));
                        }

                        using (var session = documentStore2.OpenAsyncSession())
                        {
                            var doc = session.Load<Company>("companies/1");
                            doc.Name = "Company Name 3";
                            session.SaveChanges();
                            Assert.Equal(4, session.Advanced.GetMetadataFor(doc).Value<int>("Raven-Document-Revision"));
                        }
                    }
                }
                finally
                {
                    if (File.Exists(file))
                    {
                        File.Delete(file);
                    }
                }
            }*/

        public class Comment
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class Product
        {
            public string Id { get; set; }
            public string Description { get; set; }
            public int Quantity { get; set; }
        }
    }

    public class Company
    {
        public string Name { get; set; }
        public string Id { get; set; }
    }
}
