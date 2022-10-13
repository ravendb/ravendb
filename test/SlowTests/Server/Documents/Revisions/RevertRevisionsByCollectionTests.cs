using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
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
        public async Task RevertByCollection()
        {
            var collections = new HashSet<string>() { "companies" };
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
                        token: token, collections: collections);
                }

                Assert.Equal(2, result.ScannedRevisions);
                Assert.Equal(1, result.ScannedDocuments);
                Assert.Equal(1, result.RevertedDocuments);

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
        public async Task RevertByMultipleCollections()
        {
            var collections = new HashSet<string>() { "companies", "users" };
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
                        token: token, collections: collections);
                }

                Assert.Equal(4, result.ScannedRevisions);
                Assert.Equal(2, result.ScannedDocuments);
                Assert.Equal(2, result.RevertedDocuments);

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
            var collections = new HashSet<string>() { "companies", "users" };
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
                        token: token, collections: collections);
                }

                Assert.Equal(5, result.ScannedRevisions);
                Assert.Equal(2, result.ScannedDocuments);
                Assert.Equal(2, result.RevertedDocuments);

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
            var collections = new HashSet<string>()
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
                        token: token, collections: collections);
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

                var operation = await store.Maintenance.SendAsync(new RevertRevisionsOperation(last, 60, collections));
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

                var operation = await store.Maintenance.SendAsync(new RevertRevisionsOperation(last, 60, collections));
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

                var operation = await store.Maintenance.SendAsync(new RevertRevisionsOperation(last, 60, collections));
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

        [Fact] //--
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

                var operation = await store.Maintenance.SendAsync(new RevertRevisionsOperation(last, 60, collections)); 
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

                var operation = await store.Maintenance.SendAsync(new RevertRevisionsOperation(last, 60));
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

        private class RevertRevisionsOperation : IMaintenanceOperation<OperationIdResult>
        {
            private readonly RevertRevisionsRequest _request;

            public RevertRevisionsOperation(DateTime time, long window)
            {
                _request = new RevertRevisionsRequest() { 
                    Time = time, 
                    WindowInSec = window,
                };
            }

            public RevertRevisionsOperation(DateTime time, long window, string[] collections) : this(time, window)
            {
                Debug.Assert(collections != null);
                _request.ApplyToSpecifiedCollectionsOnly = true;
                _request.Collections = collections;
            }

            public RevertRevisionsOperation(RevertRevisionsRequest request)
            {
                _request = request ?? throw new ArgumentNullException(nameof(request));
            }

            public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new RevertRevisionsCommand(_request);
            }

            private class RevertRevisionsCommand : RavenCommand<OperationIdResult>
            {
                private readonly RevertRevisionsRequest _request;

                public RevertRevisionsCommand(RevertRevisionsRequest request)
                {
                    _request = request;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/revisions/revert";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_request, ctx)).ConfigureAwait(false))
                    };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<OperationIdResult>(response);
                }
            }
        }

    }
}
