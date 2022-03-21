using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17807 : RavenTestBase
{
    public RavenDB_17807(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanUpdateDocumentByIndexWithInClauseInQuery()
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(
                new PutIndexesOperation(new[] { new IndexDefinition { Maps = { "from doc in docs.Items select new { doc.Name }" }, Name = "MyIndex" } }));

            using (var commands = store.Commands())
            {
                await commands.PutAsync("items/1", null, new { Name = "testname" },
                    new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "Items" } });

                Indexes.WaitForIndexing(store);
                var ids = new[] { "testname12", "testname1", "testname" };
                var operation = store.Operations.Send(
                    new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query = $"FROM INDEX 'MyIndex' where Name in ($p0) UPDATE {{ this.NewName = 'NewValue'; this.IsActive = false; }} ",
                            QueryParameters = new Parameters { { "p0", ids } }
                        }, new QueryOperationOptions { AllowStale = true }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                dynamic document = await commands.GetAsync("items/1");
                Assert.Equal("NewValue", document.NewName.ToString());
                Assert.Equal("False", document.IsActive.ToString());
            }
        }
    }

    [Fact]
    public async Task CanDeleteByQueryOnIndexQueryWithInClause()
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(
                new PutIndexesOperation(new[] { new IndexDefinition { Maps = { "from doc in docs.Items select new { doc.Name }" }, Name = "MyIndex" } }));

            using (var commands = store.Commands())
            {
                await commands.PutAsync("items/1", null, new { Name = "testname" },
                    new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "Items" } });

                Indexes.WaitForIndexing(store);

                var ids = new[] { "testname12", "testname1", "testname" };

                var operation = store.Operations.Send(new DeleteByQueryOperation(
                    new IndexQuery() { Query = $"FROM INDEX 'MyIndex'  where Name in ($p0)", QueryParameters = new Parameters { { "p0", ids } } },
                    new QueryOperationOptions { AllowStale = true }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                var documents = await commands.GetAsync(0, 25);
                Assert.Equal(0, documents.Count());
            }
        }
    }


    [Fact]
    public async Task CanIndexQueryWithInClause()
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(
                new PutIndexesOperation(new[] { new IndexDefinition { Maps = { "from doc in docs.Items select new { doc.Name }" }, Name = "MyIndex" } }));

            using (var commands = store.Commands())
            {
                await commands.PutAsync("items/1", null, new { Name = "testname" },
                    new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "Items" } });

                Indexes.WaitForIndexing(store);

                var ids = new[] { "testname12", "testname1", "testname" };


                var result = commands.Query(new IndexQuery()
                {
                    Query = $"FROM INDEX 'MyIndex'  where Name in ($p0)", QueryParameters = new Parameters { { "p0", ids } }
                });

                WaitForUserToContinueTheTest(store);
                Assert.NotEmpty(result.Results);
            }
        }
    }
}
