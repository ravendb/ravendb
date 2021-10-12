using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_16880 : RavenTestBase
    {
        public RavenDB_16880(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ForSpecificCollection_GetCorrectNumberOfRecords()
        {
            const string query = "from Employees";

            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                using (var session = store.OpenAsyncSession())
                {
                    var employees = await session
                        .Query<Employee>()
                        .ToListAsync();

                    var result = await Act(store, query);

                    Assert.NotNull(result);
                    Assert.NotEmpty(result.Rows);
                    Assert.Equal(employees.Count, result.Rows.Count);
                }
            }
        }

        [Fact]
        public async Task ForSpecificCollection_AndSpecificFields_GetCorrectNumberOfRecords()
        {
            const string query = "from Employees select LastName, FirstName";

            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                using (var session = store.OpenAsyncSession())
                {
                    var employees = await session.Advanced
                        .AsyncRawQuery<JObject>(query)
                        .ToArrayAsync();

                    var result = await Act(store, query);

                    Assert.NotNull(result);
                    Assert.NotEmpty(result.Rows);
                    Assert.Equal(result.Rows.Count, employees.Length);
                }
            }
        }

        [Fact]
        public async Task ForSpecificDatabase_GetCorrectCollectionNames()
        {
            const string postgresQuery =
                "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE" +
                "\r\nfrom INFORMATION_SCHEMA.tables" +
                "\r\nwhere TABLE_SCHEMA not in ('information_schema', 'pg_catalog')" +
                "\r\norder by TABLE_SCHEMA, TABLE_NAME";

            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                var collections = await store.Maintenance
                    .SendAsync(new GetCollectionStatisticsOperation());

                var result = await Act(store, postgresQuery);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Columns);
                Assert.NotEmpty(result.Rows);

                AssertDatabaseCollections(collections, result);
            }
        }

        [Fact]
        public async Task ForSpecificDatabase_AndSpecificQuery_GetCorrectSelectedFields()
        {
            const string firstField = "FirstName";
            const string secondField = "LastName";
            string query = $"from Employees select {firstField}, {secondField}";

            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                var result = await Act(store, query);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Columns);

                var columns = GetColumnNames(result);
                Assert.Contains(firstField, columns);
                Assert.Contains(secondField, columns);
            }
        }

        [Fact]
        public async Task ForSpecificDatabase_AndSpecificQuery_GetIdField()
        {
            const string firstField = "FirstName";
            const string secondField = "LastName";
            string query = $"from Employees select {firstField}, {secondField}";
            const string idField = "id()";

            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                var result = await Act(store, query);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Columns);

                var columns = GetColumnNames(result);
                Assert.Contains(idField, columns);
            }
        }

        [Fact]
        public async Task ForSpecificCollection_GetCorrectNumberOfRecord_UsingIndex()
        {
            const string query = "from index 'Orders/Totals'";

            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                var indexDefinition = new IndexDefinition
                {
                    Name = "Orders/Totals",
                    Maps =
                    {
                        @"from order in docs.Orders	
                          select new 
                          { 
                              order.Employee, 
                              order.Company,
                              Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
                          }"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));
                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var orders = await session.Advanced
                        .AsyncRawQuery<JObject>(query)
                        .ToArrayAsync();

                    var result = await Act(store, query);

                    Assert.NotNull(result);
                    Assert.NotEmpty(result.Rows);
                    Assert.Equal(orders.Length, result.Rows.Count);
                }
            }
        }

        [Fact]
        public async Task ForSpecificCollection_AndSpecificQuery_GetCorrectSelectedFields_UsingIndex()
        {
            const string query = "from index 'Orders/Totals' select Total";
            const string totalField = "Total";

            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                var indexDefinition = new IndexDefinition
                {
                    Name = "Orders/Totals",
                    Maps =
                    {
                        @"from order in docs.Orders	
                          select new 
                          { 
                              order.Employee, 
                              order.Company,
                              Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
                          }"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));
                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var orders = await session.Advanced
                        .AsyncRawQuery<JObject>(query)
                        .ToArrayAsync();

                    var result = await Act(store, query);

                    Assert.NotNull(result);
                    Assert.NotEmpty(result.Rows);
                    Assert.Equal(orders.Length, result.Rows.Count);

                    var columns = GetColumnNames(result);
                    Assert.Contains(totalField, columns);
                }
            }
        }


        private DataTable Select(
            NpgsqlConnection connection,
            string query,
            Dictionary<string, (NpgsqlDbType, object)> namedArgs = null)
        {
            using var cmd = new NpgsqlCommand(query, connection);

            if (namedArgs != null)
            {
                foreach (var (key, val) in namedArgs)
                {
                    cmd.Parameters.AddWithValue(key, val.Item1, val.Item2);
                }
            }

            using var reader = cmd.ExecuteReader();

            var dt = new DataTable();
            dt.Load(reader);

            return dt;
        }

        private string GetConnectionString(DocumentStore store)
        {
            const string correctUid = "root";
            const string correctPassword = "test";
            const string postgresPort = "5433";

            var uri = new Uri(store.Urls.First());

            var host = uri.Host;
            var database = store.Database;

            var connectionString = $"Host={host};Port={postgresPort};Database={database};Uid={correctUid};Password={correctPassword};";

            return connectionString;
        }

        private List<string> GetColumnNames(DataTable dataTable)
        {
            return dataTable.Columns
                .Cast<DataColumn>()
                .Select(x => x.ColumnName)
                .ToList();
        }


        private async Task<DataTable> Act(DocumentStore store, string query)
        {
            var connectionString = GetConnectionString(store);

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var result = Select(connection, query);

            await connection.CloseAsync();
            return result;
        }


        public static void AssertDatabaseCollections(CollectionStatistics expected, DataTable actual)
        {
            var expectedCollectionNames = expected.Collections.Keys.ToList();

            var actualCollectionNames = actual
                .AsEnumerable()
                .Select(x => x.Field<string>("table_name"))
                .ToList();

            AssertCollectionsHaveTheSameElements(expectedCollectionNames, actualCollectionNames);
        }

        public static void AssertCollectionsHaveTheSameElements(List<string> expected, List<string> actual)
        {
            Assert.All(expected, commandName => Assert.Contains(commandName, actual));
            Assert.All(actual, commandName => Assert.Contains(commandName, expected));
        }
    }
}
