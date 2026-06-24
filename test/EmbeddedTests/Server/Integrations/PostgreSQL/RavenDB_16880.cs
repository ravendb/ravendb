#if RUN_NPGSQL_TESTS
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
#pragma warning disable xUnit1051

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

        // PowerBI's DirectQuery mashup engine joins information_schema.table_constraints to
        // information_schema.key_column_usage to discover each table's primary key. Without PK
        // metadata it can't build per-row identity for relationship inference or slicer-driven
        // cross-filtering and raises `SubstituteWithIndex detected more than one row in the
        // index table matching to the current row of the original table` on any visual that
        // needs row substitution.
        //
        // Every Raven collection's PK is the synthetic `id` column (the PG-facing surface name for
        // the document identifier - see PgSyntheticColumns).
        [Fact]
        public async Task InformationSchemaConstraints_reports_synthetic_id_primary_key_for_each_collection()
        {
            // A key-discovery query PowerBI Desktop fires per table (the exact SQL varies by version).
            const string postgresQuery =
                "select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, ii.COLUMN_NAME, ii.ORDINAL_POSITION, " +
                "case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY " +
                "from INFORMATION_SCHEMA.table_constraints i " +
                "inner join INFORMATION_SCHEMA.key_column_usage ii " +
                "on i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME " +
                "and i.TABLE_SCHEMA = ii.TABLE_SCHEMA and i.TABLE_NAME = ii.TABLE_NAME " +
                "where i.TABLE_SCHEMA = 'public' and i.TABLE_NAME = 'Orders' " +
                "and i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE') " +
                "order by i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME, ii.TABLE_SCHEMA, ii.TABLE_NAME, ii.ORDINAL_POSITION";

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                var result = await Act(store, postgresQuery);

                Assert.NotNull(result);

                // Exactly one PRIMARY KEY row for Orders, on column id, at ordinal 1.
                Assert.Single(result.Rows);
                var row = (DataRow)result.Rows[0];

                Assert.Equal("id", (string)row["column_name"]);
                Assert.Equal(1, (int)row["ordinal_position"]);
                // `case when CONSTRAINT_TYPE='PRIMARY KEY' then 'Y' else 'N'` returns the literal
                // string 'Y' typed as `text` (PG oid 25). Real PostgreSQL infers the case-when
                // result type the same way - PG's internal `"char"` (oid 18) is only produced by
                // explicit `::char` casts. PowerBI's mashup engine refuses to decode binary-format
                // `"char"` as text inside RetrieveKeysForTable, so this column MUST be text.
                Assert.Equal("Y", (string)row["primary_key"]);
                Assert.Contains("Orders", (string)row["index_name"], StringComparison.Ordinal);
            }
        }

        // Datetime-shaped strings in Raven documents must be reported as `timestamp without
        // time zone` (or `timestamp with time zone` for UTC), not `text`, so PowerBI types them
        // as dates and M filters like `[OrderedAt] >= RangeStart` compare correctly. Pins the
        // InformationSchemaColumnsTable.MapDataType heuristic that mirrors RqlQuery.GenerateSchema's
        // value-inspection promotion.
        [Fact]
        public async Task InformationSchemaColumns_reports_datetime_string_fields_as_timestamp()
        {
            const string postgresQuery =
                "select column_name, data_type from information_schema.columns " +
                "where table_name = 'Orders' order by ordinal_position";

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                var result = await Act(store, postgresQuery);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Rows);

                var dataTypeByColumn = result.Rows
                    .Cast<DataRow>()
                    .ToDictionary(
                        r => (string)r["column_name"],
                        r => (string)r["data_type"],
                        StringComparer.OrdinalIgnoreCase);

                // RavenDB stores DateTime values as ISO 8601 strings, so OrderedAt / RequireAt /
                // ShippedAt must be reported as a timestamp type for PowerBI to compare them
                // against DateTime parameters.
                foreach (var col in new[] { "OrderedAt", "RequireAt", "ShippedAt" })
                {
                    Assert.True(dataTypeByColumn.TryGetValue(col, out var dataType),
                        $"Expected information_schema.columns to report a row for '{col}'.");
                    Assert.True(
                        dataType == "timestamp without time zone" || dataType == "timestamp with time zone",
                        $"Expected '{col}' to be reported as a timestamp type, was '{dataType}'.");
                }
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
            const string idField = "id";

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

        // RQL works as the wire query - including a declare-function JS projection - as long as its
        // body has no ';' (Npgsql / PowerBI split the command on ';' client-side; see the next test).
        [Fact]
        public async Task RqlDeclareFunctionWithoutSemicolonsWorksOverTheWire()
        {
            const string query = "declare function pj(o) { return { Co: o.Company } } from Orders as o select pj(o)";

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                var result = await Act(store, query);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Rows);
            }
        }

        // A declare-function body with ';' gets split client-side by Npgsql/PowerBI, so only the leading
        // fragment reaches the server. Sending that fragment directly (robust across Npgsql versions)
        // pins that it's rejected with an error echoing the text, not silently mis-run.
        [Fact]
        public async Task RqlDeclareFunctionSemicolonFragmentIsRejectedOverTheWire()
        {
            const string fragment = "declare function pj(o) { var c = o.Company";

            using (var store = GetDocumentStore())
            {
                var ex = await Assert.ThrowsAnyAsync<Exception>(async () => await Act(store, fragment));
                Assert.Contains("declare function", ex.Message, StringComparison.OrdinalIgnoreCase);
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
                    Assert.Contains("json", columnNames);
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
