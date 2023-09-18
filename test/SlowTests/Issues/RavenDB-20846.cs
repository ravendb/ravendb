using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Corax;
using FastTests.Utils;
using Microsoft.AspNetCore.Mvc.ModelBinding.Validation;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20846 : ClusterTestBase
{
    public RavenDB_20846(ITestOutputHelper output) : base(output)
    {
    }

    class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    class Product
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    [Fact]
    public async Task EnforceConfigurationForSingleCollection()
    {
        using var store = GetDocumentStore();
        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
            }
        };
        await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

        var company = new Company()
        {
            Id = "Companies/1-A",
            Name = ""
        };
        var user = new User()
        {
            Id = "Users/1-A",
            Name = ""
        };
        var product = new Product()
        {
            Id = "Product/1-A",
            Name = ""
        };


        for (int i = 0; i < 10; i++)
        {
            using var session = store.OpenAsyncSession();
            company.Name = user.Name = product.Name = $"revision{i}";
            await session.StoreAsync(company);
            await session.StoreAsync(user);
            await session.StoreAsync(product);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var companyRevCount = await session.Advanced.Revisions.GetCountForAsync(company.Id);
            Assert.Equal(10, companyRevCount);
            var userRevCount = await session.Advanced.Revisions.GetCountForAsync(user.Id);
            Assert.Equal(10, userRevCount);
        }

        configuration.Default.MinimumRevisionsToKeep = 5;
        await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

        var db = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
            await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, includeForceCreated: false, collections: new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Users", "Products" }, token: token);

        using (var session = store.OpenAsyncSession())
        {
            var companyRevCount = await session.Advanced.Revisions.GetCountForAsync(company.Id);
            Assert.Equal(10, companyRevCount);
            var userRevCount = await session.Advanced.Revisions.GetCountForAsync(user.Id);
            Assert.Equal(5, userRevCount);
            var productRevCount = await session.Advanced.Revisions.GetCountForAsync(product.Id);
            Assert.Equal(5, productRevCount);
        }
    }
}

