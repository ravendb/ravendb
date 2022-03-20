using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Integrations.PostgreSQL;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Server.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_16880 : PostgreSqlIntegrationTestBase
    {
        public RavenDB_16880(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ForSpecificCollection_GetCorrectNumberOfRecords()
        {
            const string query = "from Employees";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

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
        }

        [Fact]
        public async Task ForSpecificCollection_AndSpecificFields_GetCorrectNumberOfRecords()
        {
            const string query = "from Employees select LastName, FirstName";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                using (var session = store.OpenAsyncSession())
                {
                    var employees = await session.Advanced
                        .AsyncRawQuery<JObject>(query)
                        .ToArrayAsync();

                    var result = await Act(store, query, Server);

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

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                var collections = await store.Maintenance
                    .SendAsync(new GetCollectionStatisticsOperation());

                var result = await Act(store, postgresQuery, Server);

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

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                var result = await Act(store, query, Server);

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

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                var result = await Act(store, query, Server);

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

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

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
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var orders = await session.Advanced
                        .AsyncRawQuery<JObject>(query)
                        .ToArrayAsync();

                    var result = await Act(store, query, Server);

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

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

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
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var orders = await session.Advanced
                        .AsyncRawQuery<JObject>(query)
                        .ToArrayAsync();

                    var result = await Act(store, query, Server);

                    Assert.NotNull(result);
                    Assert.NotEmpty(result.Rows);
                    Assert.Equal(orders.Length, result.Rows.Count);

                    var columns = GetColumnNames(result);
                    Assert.Contains(totalField, columns);
                }
            }
        }


        [Fact]
        public async Task CanExportAndImportPostgreSqlIntegrationConfiguration()
        {
            using (var srcStore = GetDocumentStore())
            using (var dstStore = GetDocumentStore())

            {
                srcStore.Maintenance.Send(new ConfigurePostgreSqlOperation(new PostgreSqlConfiguration
                {
                    Authentication = new PostgreSqlAuthenticationConfiguration()
                    {
                        Users = new List<PostgreSqlUser>()
                        {
                            new PostgreSqlUser()
                            {
                                Username = "arek",
                                Password = "foo!@22"
                            }
                        }
                    }
                }));

                var record = await srcStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(srcStore.Database));

                Assert.NotNull(record.Integrations);
                Assert.NotNull(record.Integrations.PostgreSql);
                Assert.Equal(1, record.Integrations.PostgreSql.Authentication.Users.Count);

                Assert.Contains("arek", record.Integrations.PostgreSql.Authentication.Users.First().Username);
                Assert.Contains("foo!@22", record.Integrations.PostgreSql.Authentication.Users.First().Password);

                var exportFile = GetTempFileName();

                var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                record = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));

                Assert.NotNull(record.Integrations);
                Assert.NotNull(record.Integrations.PostgreSql);
                Assert.Equal(1, record.Integrations.PostgreSql.Authentication.Users.Count);

                Assert.Contains("arek", record.Integrations.PostgreSql.Authentication.Users.First().Username);
                Assert.Contains("foo!@22", record.Integrations.PostgreSql.Authentication.Users.First().Password);
            }
        }

        [Fact]
        public async Task CanTalkToSecuredServer()
        {
            var certificates = Certificates.SetupServerAuthentication(EnablePostgresSqlSettings);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            const string query = "from Employees";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
            }))
            {
                Samples.CreateNorthwindDatabase(store);

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
        }

        [Fact]
        public async Task MustNotConnectToToSecuredServerWithoutProvidingValidCredentials()
        {
            var certificates = Certificates.SetupServerAuthentication(EnablePostgresSqlSettings);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

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
        }


        [Fact]
        public async Task NpgQueryWithIntegerParametersShouldWork()
        {
            const string query = "from 'Products' where PricePerUnit > @p";

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

                using (var session = store.OpenAsyncSession())
                {
                    int pricePerUnitConditionValue = 30;

                    var products = await session.Query<Product>().Where(x => x.PricePerUnit > pricePerUnitConditionValue).ToListAsync();

                    var result = await Act(store, query, Server, parameters: new Dictionary<string, (NpgsqlDbType, object)>()
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

            DoNotReuseServer(EnablePostgresSqlSettings);

            using (var store = GetDocumentStore())
            {
                Samples.CreateNorthwindDatabase(store);

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
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var orders = await session.Advanced
                        .AsyncRawQuery<JObject>(query)
                        .ToArrayAsync();

                    var result = await Act(store, query, Server);

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
            var expectedCollectionNames = expected.Collections.Keys.Where(x => CollectionName.IsHiLoCollection(x) == false).ToList();

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
