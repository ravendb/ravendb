using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class RavenDB_23655 : RavenTestBase
{
    public RavenDB_23655(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes, LicenseRequired = true)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ExactVectorSearchHasNoEffectOnIndex(bool firstIsExact)
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Product() { Name = "test" });
            await session.SaveChangesAsync();

            var result = await session.Query<Product>()
                .Customize(c => c.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.Name),
                    v => v.ByText("test"), minimumSimilarity: 0.7f, isExact: firstIsExact)
                .ToArrayAsync();
            Assert.Single(result);
        }

        var command = new PutDatabaseStudioConfigurationCommand(new ServerWideStudioConfiguration() { DisableAutoIndexCreation = true, }, store.Database,
            RaftIdGenerator.NewId());
        await Server.ServerStore.SendToLeaderAsync(command);

        var dbInstance = await GetDocumentDatabaseInstanceFor(store, store.Database);
        using (var context = QueryOperationContext.ShortTermSingleUse(dbInstance))
        {
            var result = await dbInstance.QueryRunner.ExecuteQuery(
                new IndexQueryServerSide("from  'Products' where exact(vector.search(embedding.text(Name), 'test', 0.3))") { DisableAutoIndexCreation = true }, context,
                null,
                OperationCancelToken.None);
            
            Assert.Equal(1, result.TotalResults);
        }
    }
}
