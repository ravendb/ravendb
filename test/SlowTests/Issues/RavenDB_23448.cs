using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23448(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void CanQueryTextualValuesInVectorSearchWhenFieldConfigurationIsNotExplicitlySet()
    {
        using var store = GetDocumentStore(new Options() { RunInMemory = false, DeleteDatabaseOnDispose = true, Path = NewDataPath() });
        new Index().Execute(store);

        using (var session = store.OpenSession())
        {
            session.Store(new Product { Name = "Product 1" });
            session.SaveChanges();
            Indexes.WaitForIndexing(store);

            var result = session.Advanced.DocumentQuery<Product, Index>()
                .VectorSearch(p => p.WithField("Vector"),
                    v => v.ByText("Product 1"))
                .FirstOrDefault();

            Assert.NotNull(result);
        }

        var status = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
        Assert.Equal(true, status.Disabled);
        status = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
        Assert.Equal(false, status.Disabled);

        using (var session = store.OpenSession())
        {
            var result = session.Advanced.DocumentQuery<Product, Index>()
                .VectorSearch(p => p.WithField("Vector"),
                    v => v.ByText("Product 1"))
                .FirstOrDefault();

            Assert.NotNull(result);

            result = session.Advanced.RawQuery<Product>(@"from index Index where vector.search(Vector, $p0)")
                .AddParameter("$p0", "Product 1")
                .FirstOrDefault();

            Assert.NotNull(result);
        }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void CanQueryNumericalValuesInVectorSearchWhenFieldConfigurationIsNotExplicitlySet()
    {
        float[] vec = [0.1f, 0.2f, 0.3f];
        using var store = GetDocumentStore(new Options() { RunInMemory = false, DeleteDatabaseOnDispose = true, Path = NewDataPath() });
        new IndexWithEmbedding().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new ProductEmbedding() { Vector = vec });
            session.SaveChanges();
            Indexes.WaitForIndexing(store);

            var result = session.Advanced.DocumentQuery<ProductEmbedding, IndexWithEmbedding>()
                .VectorSearch(p => p.WithField(x => x.Vector),
                    v => v.ByEmbedding(vec))
                .FirstOrDefault();

            Assert.NotNull(result);
        }

        var status = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
        Assert.Equal(true, status.Disabled);
        status = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
        Assert.Equal(false, status.Disabled);

        using (var session = store.OpenSession())
        {
            var result = session.Advanced.DocumentQuery<ProductEmbedding, IndexWithEmbedding>()
                .VectorSearch(p => p.WithField(x => x.Vector),
                    v => v.ByEmbedding(vec))
                .FirstOrDefault();

            Assert.NotNull(result);
        }
    }

    private class Index : AbstractIndexCreationTask<Product>
    {
        public Index()
        {
            Map = products => from p in products
                select new { Vector = CreateVector(p.Name) };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexWithEmbedding : AbstractIndexCreationTask<ProductEmbedding>
    {
        public IndexWithEmbedding()
        {
            Map = products => from p in products
                select new { Vector = CreateVector(p.Vector) };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class ProductEmbedding
    {
        public float[] Vector { get; set; }
    }
}
