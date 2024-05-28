#if NET8_0
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NpgsqlTypes;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_16880 : PostgreSqlIntegrationTestBase
    {

        [Fact]
        public async Task ForSpecificCollection_GetCorrectNumberOfRecords()
        {
            const string query = "from Employees";

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

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
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

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
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

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
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

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
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

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
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

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
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

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

        [Fact(Skip = "RavenDB-22360 / RavenDB-17749 Talking to secured server isn't easily achievable in EmbeddedTests")]
        public void CanTalkToSecuredServer()
        {
            /*
            var certificates = Constants.Certificates.SetupServerAuthentication(EnablePostgresSqlSettings);
            var dbName = GetDatabaseName();
            var adminCert = Constants.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            const string query = "from Employees";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
            }))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                store.Maintenance.Send(new ConfigurePostgreSqlOperation(new PostgreSqlConfiguration
                {
                    Authentication = new PostgreSqlAuthenticationConfiguration()
                    {
                        Users = new List<PostgreSqlUser>()
                        {
                            new PostgreSqlUser()
                            {
                                Username = CorrectUser,
                                Password = CorrectPassword
                            }
                        }
                    }
                }));

                using (var session = store.OpenAsyncSession())
                {
                    var employees = await session
                        .Query<Employee>()
                        .ToListAsync();

                    var result = await Act(store, query, Server);

                    Assert.NotNull(result);
                    Assert.NotEmpty(result.Rows);
                    Assert.Equal(employees.Count, result.Rows.Count);
                }
            }
            */
        }

        [Fact(Skip = "RavenDB-22360 / RavenDB-17749 Talking to secured server isn't easily achievable in EmbeddedTests")]
        public void MustNotConnectToToSecuredServerWithoutProvidingValidCredentials()
        {
            /*
            var certificates = Constants.Certificates.SetupServerAuthentication(EnablePostgresSqlSettings);
            var dbName = GetDatabaseName();
            var adminCert = Constants.Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            const string query = "from Employees";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
            }))
            {
                var npgSqlException = await Assert.ThrowsAsync<NpgsqlException>(async () => await Act(store, query, Server, forceSslMode: false));

                Assert.Equal("No password has been provided but the backend requires one (in cleartext)", npgSqlException.Message);

                var pgException = await Assert.ThrowsAsync<PostgresException>(async () => await Act(store, query, Server));

                Assert.Equal("0P000: role \"root\" does not exist", pgException.Message);

                store.Maintenance.Send(new ConfigurePostgreSqlOperation(new PostgreSqlConfiguration
                {
                    Authentication = new PostgreSqlAuthenticationConfiguration()
                    {
                        Users = new List<PostgreSqlUser>()
                       {
                           new PostgreSqlUser()
                           {
                               Username = CorrectUser,
                               Password = "incorrect_password"
                           }
                       }
                    }
                }));

                pgException = await Assert.ThrowsAsync<PostgresException>(async () => await Act(store, query, Server));

                Assert.Equal("28P01: password authentication failed for user \"root\"", pgException.Message);
            }
            */
        }


        [Fact]
        public async Task NpgQueryWithIntegerParametersShouldWork()
        {
            const string query = "from 'Products' where PricePerUnit > @p";

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                using (var session = store.OpenAsyncSession())
                {
                    int pricePerUnitConditionValue = 30;

                    var products = await session.Query<Product>().Where(x => x.PricePerUnit > pricePerUnitConditionValue).ToListAsync();

                    var result = await Act(store, query, parameters: new Dictionary<string, (NpgsqlDbType, object)>()
                    {
                        {"p", (NpgsqlDbType.Integer, pricePerUnitConditionValue)}
                    });

                    Assert.NotNull(result);
                    Assert.NotEmpty(result.Rows);
                    Assert.Equal(products.Count, result.Rows.Count);
                }
            }
        }

        [Fact]
        public async Task CanGetCorrectNumberOfRecordAndFieldNameUsingMapReduceIndex()
        {
            const string query = "from index 'Orders/ByCompany'";

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                var indexDefinition = new IndexDefinition
                {
                    Name = "Orders/ByCompany",
                    Maps =
                    {
                        @"from order in docs.Orders
select new
{
    order.Company,
    Count = 1,
    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}"
                    },
                    Reduce = @"from result in results
group result by result.Company 
into g
select new
{
    Company = g.Key,
    Count = g.Sum(x => x.Count),
    Total = g.Sum(x => x.Total)
}"
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

                    var columnNames = GetColumnNames(result);

                    Assert.Equal(4, columnNames.Count);
                    Assert.Contains("Company", columnNames);
                    Assert.Contains("Count", columnNames);
                    Assert.Contains("Total", columnNames);
                    Assert.Contains(Constants.Documents.Querying.Fields.PowerBIJsonFieldName, columnNames);
                }
            }
        }

        private List<string> GetColumnNames(DataTable dataTable)
        {
            return dataTable.Columns
                .Cast<DataColumn>()
                .Select(x => x.ColumnName)
                .ToList();
        }

        public static void AssertDatabaseCollections(CollectionStatistics expected, DataTable actual)
        {
            var expectedCollectionNames = expected.Collections.Keys.Where(x => string.Equals(x, "@hilo", StringComparison.OrdinalIgnoreCase) == false).ToList();

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
#endif
