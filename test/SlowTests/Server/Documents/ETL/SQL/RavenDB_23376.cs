using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Data.SqlClient;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.Migration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL.SQL
{
    public class RavenDB_23376 : SqlAwareTestBase
    {
        public RavenDB_23376(ITestOutputHelper output) : base(output)
        {
        }

        [RequiresMsSqlRetryFact(delayBetweenRetriesMs: 1000)]
        public async Task ShouldPropagateDeletionsOfArtificialDocuments_BasicScenario()
        {
            using (var store = GetDocumentStore())
            {
                using (WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateCategorySummarySchema(connectionString);

                    await new ProductsByCategory().ExecuteAsync(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Product { Name = "Product1", Category = "Electronics", PricePerUnit = 100 });
                        await session.StoreAsync(new Product { Name = "Product2", Category = "Electronics", PricePerUnit = 200 });
                        await session.StoreAsync(new Product { Name = "Product3", Category = "Books", PricePerUnit = 50 });
                        await session.SaveChangesAsync();
                    }

                    await Indexes.WaitForIndexingAsync(store);

                    using (var session = store.OpenSession())
                    {
                        WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, interval: 500);
                    }

                    var etlDone = Etl.WaitForEtlToComplete(store);

                    SetupSqlEtlForCategories(store, connectionString);

                    await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = "SELECT COUNT(*) FROM ProductsByCategories";
                            Assert.Equal(2, dbCommand.ExecuteScalar());

                            dbCommand.CommandText = "SELECT Category, TotalPrice FROM ProductsByCategories ORDER BY Category";
                            using (var reader = dbCommand.ExecuteReader())
                            {
                                Assert.True(reader.Read());
                                Assert.Equal("Books", reader.GetString(0));
                                Assert.Equal(50m, reader.GetDecimal(1));

                                Assert.True(reader.Read());
                                Assert.Equal("Electronics", reader.GetString(0));
                                Assert.Equal(300m, reader.GetDecimal(1));

                                Assert.False(reader.Read());
                            }
                        }
                    }

                    etlDone.Reset();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete("products/1-A");
                        session.Delete("products/2-A");
                        session.Delete("products/3-A");
                        await session.SaveChangesAsync();
                    }

                    await Indexes.WaitForIndexingAsync(store);

                    using (var session = store.OpenSession())
                    {
                        WaitForValue(() => session.Query<ProductsByCategories>().Count(), 0, interval: 500);
                    }

                    await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = "SELECT COUNT(*) FROM ProductsByCategories";
                            Assert.Equal(0, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

        [RequiresMsSqlRetryFact(delayBetweenRetriesMs: 1000)]
        public async Task ShouldPropagateDeletionsOfArtificialDocuments_IndexUpdateScenario()
        {
            using (var store = GetDocumentStore())
            {
                using (WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateCategorySummarySchema(connectionString);

                    await new ProductsByCategory().ExecuteAsync(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Product { Name = "Product1", Category = "Electronics", PricePerUnit = 100 });
                        await session.StoreAsync(new Product { Name = "Product2", Category = "Electronics", PricePerUnit = 200 });
                        await session.StoreAsync(new Product { Name = "Product3", Category = "Books", PricePerUnit = 50 });
                        await session.SaveChangesAsync();
                    }

                    await Indexes.WaitForIndexingAsync(store);

                    using (var session = store.OpenSession())
                    {
                        WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, interval: 500);
                    }

                    var etlDone = Etl.WaitForEtlToComplete(store);

                    SetupSqlEtlForCategories(store, connectionString);

                    await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = "SELECT COUNT(*) FROM ProductsByCategories";
                            Assert.Equal(2, dbCommand.ExecuteScalar());
                        }
                    }

                    await new ProductsByCategoryUpdated().ExecuteAsync(store);

                    await Indexes.WaitForIndexingAsync(store);

                    using (var session = store.OpenSession())
                    {
                        WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, interval: 500);
                    }

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            var count = WaitForValue(() =>
                            {
                                dbCommand.CommandText = "SELECT COUNT(*) FROM ProductsByCategories";
                                return (int)dbCommand.ExecuteScalar();
                            }, 2, timeout: 60_000, interval: 500);

                            Assert.Equal(2, count);

                            dbCommand.CommandText = "SELECT Category FROM ProductsByCategories ORDER BY Category";
                            using (var reader = dbCommand.ExecuteReader())
                            {
                                Assert.True(reader.Read());
                                Assert.Equal("Books", reader.GetString(0));

                                Assert.True(reader.Read());
                                Assert.Equal("Electronics", reader.GetString(0));

                                Assert.False(reader.Read());
                            }
                        }
                    }
                }
            }
        }

        private void CreateCategorySummarySchema(string connectionString)
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = connectionString;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = @"
CREATE TABLE [dbo].[ProductsByCategories]
(
    [Id] [nvarchar](50) NOT NULL,
    [Category] [nvarchar](255) NOT NULL,
    [TotalPrice] [decimal](18, 2) NOT NULL,
    [Count] [int] NOT NULL
)";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        private void SetupSqlEtlForCategories(DocumentStore store, string connectionString)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to SQL DB";

            var script = @"
loadToProductsByCategories({
    Id: id(this),
    Category: this.Category,
    TotalPrice: this.TotalPrice,
    Count: this.Count
});
";

            Etl.AddEtl(store, new SqlEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                SqlTables = new List<SqlEtlTable>
                {
                    new SqlEtlTable { TableName = "ProductsByCategories", DocumentIdColumn = "Id" }
                },
                Transforms =
                {
                    new Transformation()
                    {
                        Name = "ProductsByCategories",
                        Collections = new List<string> { "ProductsByCategories" },
                        Script = script
                    }
                }
            }, new SqlConnectionString
            {
                Name = connectionStringName,
                ConnectionString = connectionString,
                FactoryName = "Microsoft.Data.SqlClient"
            });
        }

        private class ProductsByCategories
        {
            public string Category { get; set; }
            public decimal TotalPrice { get; set; }
            public int Count { get; set; }
        }

        private class ProductsByCategory : AbstractIndexCreationTask<Product, ProductsByCategories>
        {
            public ProductsByCategory()
            {
                Map = products =>
                    from product in products
                    select new ProductsByCategories
                    {
                        Category = product.Category,
                        TotalPrice = product.PricePerUnit,
                        Count = 1
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Category
                    into g
                    select new ProductsByCategories
                    {
                        Category = g.Key,
                        TotalPrice = g.Sum(x => x.TotalPrice),
                        Count = g.Sum(x => x.Count)
                    };

                OutputReduceToCollection = "ProductsByCategories";
            }
        }

        private class ProductsByCategoryUpdated : AbstractIndexCreationTask<Product, ProductsByCategories>
        {
            public override string IndexName => "ProductsByCategory";

            public ProductsByCategoryUpdated()
            {
                Map = products =>
                    from product in products
                    select new ProductsByCategories
                    {
                        Category = product.Category,
                        TotalPrice = product.PricePerUnit,
                        Count = 1
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Category
                    into g
                    select new ProductsByCategories
                    {
                        Category = g.Key,
                        TotalPrice = g.Sum(x => x.TotalPrice),
                        Count = g.Sum(x => x.Count)
                    };

                OutputReduceToCollection = "ProductsByCategories";

                // to make the index different
                AdditionalSources = new Dictionary<string, string>
                {
                    { "CustomScript", "// Updated version" }
                };
            }
        }
    }
}
