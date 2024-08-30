using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Bugs.Indexing;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Server.Documents.Revisions
{
    public class RevertRevisionsByCollectionTests : ReplicationTestBase
    {
        public RevertRevisionsByCollectionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RevertByMultipleCollections_ShouldRemoveDocWhichCreatedAfterTheMinDate()
        {
            var batchSizeLimitInBytes = 32 * 1_024; //32kb
            var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "companies", "users" };
            var names = new[] { GenerateRandomString(batchSizeLimitInBytes / 2), GenerateRandomString(batchSizeLimitInBytes / 2 + 1), GenerateRandomString(batchSizeLimitInBytes / 2), };
            using var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 1
            });

            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            database.DocumentsStorage.RevisionsStorage.SizeLimitInBytes = batchSizeLimitInBytes;
            await RevisionsHelper.SetupRevisions(store, Server.ServerStore);
            
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Company { Name = names[0] }, "companies/1");

                await session.StoreAsync(new Company { Name = names[1] }, "companies/2");

                await session.StoreAsync(new User { Name = names[2] }, "users/1");

                await session.SaveChangesAsync();
            }

            var last = DateTime.UtcNow;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Company { Name = "abc" }, "companies/1");

                await session.StoreAsync(new Company { Name = "abc" }, "companies/2");

                await session.StoreAsync(new User { Name = "abc" }, "users/1");

                await session.StoreAsync(new User { Name = "abc" }, "users/2");

                await session.SaveChangesAsync();
            }

            using (var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown, CancellationToken.None))
            {
                var result = (RevertResult)await database.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                    token: token, collections: collections);
            }

            using (var session = store.OpenAsyncSession())
            {
                var o1 = await session.LoadAsync<Company>("companies/1");
                var o2 = await session.LoadAsync<Company>("companies/2");
                var o3 = await session.LoadAsync<User>("users/1");
                var o4 = await session.LoadAsync<User>("users/2");
                Assert.NotNull(o1);
                Assert.NotNull(o2);
                Assert.NotNull(o3);
                Assert.Null(o4);

                var revisions_c1 = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");
                Assert.Equal(3, revisions_c1.Count);
                Assert.Equal(names[0], revisions_c1[0].Name);
                Assert.Equal("abc", revisions_c1[1].Name);
                Assert.Equal(names[0], revisions_c1[2].Name);

                var revisions_c2 = await session.Advanced.Revisions.GetForAsync<Company>("companies/2");
                Assert.Equal(3, revisions_c2.Count);
                Assert.Equal(names[1], revisions_c2[0].Name);
                Assert.Equal("abc", revisions_c2[1].Name);
                Assert.Equal(names[1], revisions_c2[2].Name);

                var revisions_u1 = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                Assert.Equal(3, revisions_u1.Count);
                Assert.Equal(names[2], revisions_u1[0].Name);
                Assert.Equal("abc", revisions_u1[1].Name);
                Assert.Equal(names[2], revisions_u1[2].Name);

                var revisions_u2 = await session.Advanced.Revisions.GetForAsync<User>("users/2");
                Assert.Null(revisions_u2[0].Name);
                Assert.Equal("abc", revisions_u2[1].Name);
            }

        }

        private string GenerateRandomString(int size)
        {
            var ran = new Random();
            StringBuilder sb = new StringBuilder(size);

            for (int i = 0; i < size; i++)
            {
                sb.Append(Convert.ToChar(ran.NextInt64(65,90)));
            }

            return sb.ToString();
        }

        [Fact]
        public async Task RevertByCollection()
        {
            var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "companies" };
            var company = new Company { Name = "Company Name" };
            var user = new User { Name = "User Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    user.Name = "Shahar";
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        collections, token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(2, usersRevisions.Count);

                    Assert.Equal("Shahar", usersRevisions[0].Name);
                    Assert.Equal("User Name", usersRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task RevertByCollectionForNonRevisionsConfiguredCollection()
        {
            var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Products", "User" };
            var product = new Product { Name = "Product Name" };
            var user = new User { Name = "User Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                var config = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = true, MinimumRevisionsToKeep = 5 },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Users"] = new RevisionsCollectionConfiguration { Disabled = false, PurgeOnDelete = true, MinimumRevisionsToKeep = 123 },
                        ["People"] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 },
                        ["Comments"] = new RevisionsCollectionConfiguration { Disabled = true },
                    }
                };
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, modifyConfiguration: c =>
                {
                    c.Default = null;
                    c.Collections = config.Collections;
                });
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    product.Name = "RavenDB";
                    user.Name = "Shahar";
                    await session.StoreAsync(product);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        collections, token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal("RavenDB", (await session.LoadAsync<Product>(product.Id)).Name);

                    var productsRevisions = await session.Advanced.Revisions.GetForAsync<Product>(product.Id);
                    Assert.Equal(0, productsRevisions.Count);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(2, usersRevisions.Count);

                    Assert.Equal("Shahar", usersRevisions[0].Name);
                    Assert.Equal("User Name", usersRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task RevertByMultipleCollections()
        {
            var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "companies", "users" };
            var company = new Company { Name = "Company Name" };
            var user = new User { Name = "User Name" };
            var contact = new Contact { FirstName = "User Name" };

            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    user.Name = "Shahar";
                    contact.FirstName = "Shahar";
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        collections, token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(3, usersRevisions.Count);

                    Assert.Equal("User Name", usersRevisions[0].Name);
                    Assert.Equal("Shahar", usersRevisions[1].Name);
                    Assert.Equal("User Name", usersRevisions[2].Name);

                    var contactRevisions = await session.Advanced.Revisions.GetForAsync<Contact>(contact.Id);
                    Assert.Equal(2, contactRevisions.Count);

                    Assert.Equal("Shahar", contactRevisions[0].FirstName);
                    Assert.Equal("User Name", contactRevisions[1].FirstName);
                }
            }
        }

        [Fact]
        public async Task RevertByMultipleExistingAndDeletedCollections()
        {
            var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "companies", "users" };
            var company = new Company { Name = "Company Name" };
            var user = new User { Name = "User Name" };
            var contact = new Contact { FirstName = "User Name" };

            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    user.Name = "Shahar";
                    contact.FirstName = "Shahar";
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(company.Id);
                    session.SaveChanges();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        collections, token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(4, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal(null, companiesRevisions[1].Name); // representing the delete (tombstone)
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[2].Name);
                    Assert.Equal("Company Name", companiesRevisions[3].Name);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(3, usersRevisions.Count);

                    Assert.Equal("User Name", usersRevisions[0].Name);
                    Assert.Equal("Shahar", usersRevisions[1].Name);
                    Assert.Equal("User Name", usersRevisions[2].Name);

                    var contactRevisions = await session.Advanced.Revisions.GetForAsync<Contact>(contact.Id);
                    Assert.Equal(2, contactRevisions.Count);

                    Assert.Equal("Shahar", contactRevisions[0].FirstName);
                    Assert.Equal("User Name", contactRevisions[1].FirstName);
                }
            }
        }

        [Fact]
        public async Task RevertByWrongCollection()
        {
            var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "companies", 
                "notExistingCollection" // not existing collection
            };
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None)) 
                { 
                    var result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        collections, token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);
                }
            }
        }

        [Fact]
        public async Task RevertByCollection_EndPointCheck()
        {
            var collections = new string[] { "companies" };
            var company = new Company { Name = "Company Name" };
            var user = new User { Name = "User Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    user.Name = "Shahar";
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(last, 60, collections));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(2, usersRevisions.Count);

                    Assert.Equal("Shahar", usersRevisions[0].Name);
                    Assert.Equal("User Name", usersRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task RevertByCollectionForNonRevisionsConfiguredCollection_EndPointCheck()
        {
            var collections = new string[] { "Products", "User" };
            var product = new Product { Name = "Product Name" };
            var user = new User { Name = "User Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                var config = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = true, MinimumRevisionsToKeep = 5 },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Users"] = new RevisionsCollectionConfiguration { Disabled = false, PurgeOnDelete = true, MinimumRevisionsToKeep = 123 },
                        ["People"] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 },
                        ["Comments"] = new RevisionsCollectionConfiguration { Disabled = true },
                    }
                };
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, modifyConfiguration: c =>
                {
                    c.Default = null;
                    c.Collections = config.Collections;
                });
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    product.Name = "RavenDB";
                    user.Name = "Shahar";
                    await session.StoreAsync(product);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(last, 60, collections));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal("RavenDB", (await session.LoadAsync<Product>(product.Id)).Name);

                    var productsRevisions = await session.Advanced.Revisions.GetForAsync<Product>(product.Id);
                    Assert.Equal(0, productsRevisions.Count);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(2, usersRevisions.Count);
                
                    Assert.Equal("Shahar", usersRevisions[0].Name);
                    Assert.Equal("User Name", usersRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task RevertByMultipleCollections_EndPointCheck()
        {
            var collections = new string[] { "companies", "users" };
            var company = new Company { Name = "Company Name" };
            var user = new User { Name = "User Name" };
            var contact = new Contact { FirstName = "User Name" };

            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    user.Name = "Shahar";
                    contact.FirstName = "Shahar";
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                }

                var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(last, 60, collections));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(3, usersRevisions.Count);

                    Assert.Equal("User Name", usersRevisions[0].Name);
                    Assert.Equal("Shahar", usersRevisions[1].Name);
                    Assert.Equal("User Name", usersRevisions[2].Name);

                    var contactRevisions = await session.Advanced.Revisions.GetForAsync<Contact>(contact.Id);
                    Assert.Equal(2, contactRevisions.Count);

                    Assert.Equal("Shahar", contactRevisions[0].FirstName);
                    Assert.Equal("User Name", contactRevisions[1].FirstName);
                }
            }
        }

        [Fact]
        public async Task RevertByMultipleExistingAndDeletedCollections_EndPointCheck()
        {
            var collections = new string[] { "companies", "users" };
            var company = new Company { Name = "Company Name" };
            var user = new User { Name = "User Name" };
            var contact = new Contact { FirstName = "User Name" };

            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    user.Name = "Shahar";
                    contact.FirstName = "Shahar";
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.StoreAsync(contact);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(company.Id);
                    session.SaveChanges();
                }

                var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(last, 60, collections));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(4, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal(null, companiesRevisions[1].Name); // representing the delete (tombstone)
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[2].Name);
                    Assert.Equal("Company Name", companiesRevisions[3].Name);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(3, usersRevisions.Count);

                    Assert.Equal("User Name", usersRevisions[0].Name);
                    Assert.Equal("Shahar", usersRevisions[1].Name);
                    Assert.Equal("User Name", usersRevisions[2].Name);

                    var contactRevisions = await session.Advanced.Revisions.GetForAsync<Contact>(contact.Id);
                    Assert.Equal(2, contactRevisions.Count);

                    Assert.Equal("Shahar", contactRevisions[0].FirstName);
                    Assert.Equal("User Name", contactRevisions[1].FirstName);
                }
            }
        }

        [Fact]
        public async Task RevertByWrongCollection_EndPointCheck()
        {
            var collections = new string[]
            {
                "companies",
                "notExistingCollection" // not existing collection
            };
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(last, 60, collections)); 
                var result = await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);
                }
            }
        }

        [Fact]
        public async Task Revert_EndPointCheck()
        {
            var company = new Company { Name = "Company Name" };
            var user = new User { Name = "User Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    user.Name = "Shahar";
                    await session.StoreAsync(company);
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(last, 60));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);

                    var usersRevisions = await session.Advanced.Revisions.GetForAsync<Company>(user.Id);
                    Assert.Equal(3, usersRevisions.Count);

                    Assert.Equal("User Name", usersRevisions[0].Name);
                    Assert.Equal("Shahar", usersRevisions[1].Name);
                    Assert.Equal("User Name", usersRevisions[2].Name);
                }
            }
        }

        class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
