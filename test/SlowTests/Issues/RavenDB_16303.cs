using System;
using System.Data.SqlClient;
using System.Threading;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.Migration;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16303 : SqlAwareTestBase
    {
        public RavenDB_16303(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public void CanCreateSqlEtlTransformScriptWithGuidWhenTableTypeGuid(MigrationProvider provider)
        {
            using (var store = GetDocumentStore())
            {
                using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    GetCreateTableWithUniqueType(provider, connectionString);
                    CheckCount(provider, connectionString, 0);

                    var sqlEtlConfigurationName = "test_" + Guid.NewGuid();
                    var connectionStringName = $"test_{store.Database}";
                    var operation = new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
                    {
                        Name = connectionStringName,
                        FactoryName = GetFactoryName(provider),
                        ConnectionString = connectionString
                    });

                    store.Maintenance.Send(operation);

                    var etlDone = new ManualResetEventSlim();
                    var configuration = new SqlEtlConfiguration
                    {
                        Name = sqlEtlConfigurationName,
                        ConnectionStringName = connectionStringName,
                        SqlTables = { new SqlEtlTable { TableName = "TestGuidEtls", DocumentIdColumn = "Id" }, },
                        Transforms =
                        {
                            new Transformation
                            {
                                Name = "TestGuidEtls",
                                Collections = {"TestGuidEtls"},
                                Script = @$"
var item = {{
Id: id(this),
Guid: {{ Value: this.Guid, Type: 'Guid' }}
}};

loadToTestGuidEtls(item);"
                            },
                        }
                    };

                    store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(configuration));
                    var database = GetDatabase(store.Database).Result;
                    var errors = 0;
                    database.EtlLoader.BatchCompleted += x =>
                    {
                        if (x.ConfigurationName == sqlEtlConfigurationName && x.TransformationName == "TestGuidEtls")
                        {
                            if (x.Statistics.LoadSuccesses > 0)
                            {
                                errors = x.Statistics.LoadErrors;
                                etlDone.Set();
                            }
                        }
                    };
                    var guid = Guid.NewGuid();
                    using (var session = store.OpenSession())
                    {
                        session.Store(new TestGuidEtl() { Guid = guid });
                        session.SaveChanges();
                    }

                    etlDone.Wait(TimeSpan.FromMinutes(5));
                    Assert.Equal(0, errors);

                    CheckCount(provider, connectionString, 1);
                    CheckSelectGuidCommand(provider, guid, connectionString);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresMySqlInlineData]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        public void CanCreateSqlEtlTransformScriptWithGuidWhenTableTypeVarchar(MigrationProvider provider)
        {
            using (var store = GetDocumentStore())
            {
                using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateTableWithVarchar(provider, connectionString);

                    CheckCount(provider, connectionString, 0);
                    var sqlEtlConfigurationName = "test_" + Guid.NewGuid();
                    var connectionStringName = $"test_{store.Database}";
                    var operation = new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
                    {
                        Name = connectionStringName,
                        FactoryName = GetFactoryName(provider),
                        ConnectionString = connectionString
                    });

                    store.Maintenance.Send(operation);

                    var etlDone = new ManualResetEventSlim();
                    var configuration = new SqlEtlConfiguration
                    {
                        Name = sqlEtlConfigurationName,
                        ConnectionStringName = connectionStringName,
                        SqlTables = { new SqlEtlTable { TableName = "TestGuidEtls", DocumentIdColumn = "Id" }, },
                        Transforms =
                        {
                            new Transformation
                            {
                                Name = "TestGuidEtls",
                                Collections = {"TestGuidEtls"},
                                Script = @$"
var item = {{
Id: id(this),
Guid: this.Guid
}};

loadToTestGuidEtls(item);"
                            },
                        }
                    };

                    store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(configuration));
                    var database = GetDatabase(store.Database).Result;
                    var errors = 0;
                    database.EtlLoader.BatchCompleted += x =>
                    {
                        if (x.ConfigurationName == sqlEtlConfigurationName && x.TransformationName == "TestGuidEtls")
                        {
                            if (x.Statistics.LoadSuccesses > 0)
                            {
                                errors = x.Statistics.LoadErrors;
                                etlDone.Set();
                            }
                        }
                    };

                    var guid = Guid.NewGuid();
                    using (var session = store.OpenSession())
                    {
                        session.Store(new TestGuidEtl() { Guid = guid });
                        session.SaveChanges();
                    }

                    etlDone.Wait(TimeSpan.FromMinutes(5));
                    Assert.Equal(0, errors);

                    CheckCount(provider, connectionString, 1);
                    CheckSelectCommand(provider, guid, connectionString);
                }
            }
        }

        public class TestGuidEtl
        {
            public Guid Guid { get; set; }
        }

        private static string GetFactoryName(MigrationProvider provider)
        {
            switch (provider)
            {
                case MigrationProvider.Oracle:
                    return @"Oracle.ManagedDataAccess.Client";
                case MigrationProvider.MsSQL:
                    return @"System.Data.SqlClient";
                case MigrationProvider.MySQL:
                    return @"MySql.Data.MySqlClient";
                case MigrationProvider.NpgSQL:
                    return @"Npgsql";
                default:
                    throw new InvalidOperationException(nameof(provider));
            }
        }

        private static void CreateTableWithVarchar(MigrationProvider provider, string connectionString)
        {
            switch (provider)
            {
                case MigrationProvider.Oracle:
                    {
                        using (var connection = new OracleConnection(connectionString))
                        {
                            connection.Open();

                            using (var dbCommand = connection.CreateCommand())
                            {
                                dbCommand.CommandTimeout = 10 * 60;
                                dbCommand.CommandText = @"
                        CREATE TABLE ""TestGuidEtls"" (
                            ""Id"" varchar2(40) NOT NULL,
                            ""Guid"" varchar2(36)
                        );";
                                dbCommand.ExecuteNonQuery();
                            }

                            connection.Close();
                        }
                    }
                    return;
                case MigrationProvider.MsSQL:
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = @"
                        CREATE TABLE ""TestGuidEtls"" (
                            ""Id"" varchar(40) NOT NULL,
                            ""Guid"" varchar(36)
                        );";

                            dbCommand.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MySQL:
                    using (var connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();

                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = @"
                        CREATE TABLE `TestGuidEtls` (
                            `Id` varchar(40) NOT NULL,
                            `Guid` varchar(36)
                        );";
                            dbCommand.ExecuteNonQuery();
                        }

                        connection.Close();
                    }
                    return;
                case MigrationProvider.NpgSQL:
                    using (var connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();

                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = @"DROP TABLE ""TestGuidEtls"";
                        CREATE TABLE ""TestGuidEtls"" (
                            ""Id"" varchar(40) NOT NULL,
                            ""Guid"" varchar(36)
                        );";

                            dbCommand.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    return;
                default:
                    throw new InvalidOperationException(nameof(provider));
            }
        }

        private static void GetCreateTableWithUniqueType(MigrationProvider provider, string connectionString)
        {
            switch (provider)
            {
                case MigrationProvider.Oracle:
                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = @"
                        CREATE TABLE ""TestGuidEtls"" (
                            ""Id"" varchar2(40) NOT NULL,
                            ""Guid"" raw(16)
                        );";
                            dbCommand.ExecuteNonQuery();
                        }

                        connection.Close();
                    }
                    return;
                case MigrationProvider.MsSQL:
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = @"
                    CREATE TABLE ""TestGuidEtls"" (
                        ""Id"" varchar(40) NOT NULL,
                        ""Guid"" uniqueidentifier
                    );";

                            dbCommand.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MySQL:
                    using (var connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();

                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = @"
                        CREATE TABLE `TestGuidEtls` (
                            `Id` varchar(40) NOT NULL,
                            `Guid` binary(16)
                        );";
                            dbCommand.ExecuteNonQuery();
                        }

                        connection.Close();
                    }
                    return;
                case MigrationProvider.NpgSQL:
                    using (var connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();

                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = @"DROP TABLE ""TestGuidEtls"";
                    CREATE TABLE ""TestGuidEtls"" (
                        ""Id"" varchar(40) NOT NULL,
                        ""Guid"" UUID
                    );";

                            dbCommand.ExecuteNonQuery();
                        }
                        connection.Close();
                    }
                    return;
                default:
                    throw new InvalidOperationException(nameof(provider));
            }
        }

        private static void CheckCount(MigrationProvider provider, string connectionString, int expected)
        {
            switch (provider)
            {
                case MigrationProvider.Oracle:
                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT COUNT(*) FROM \"TestGuidEtls\"";
                            var res = dbCommand.ExecuteScalar();
                            Assert.Equal(expected, Convert.ToInt32(res));
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MsSQL:
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT COUNT(*) FROM \"TestGuidEtls\"";
                            var res = dbCommand.ExecuteScalar();
                            Assert.Equal(expected, Convert.ToInt32(res));
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MySQL:
                    using (var connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT COUNT(*) FROM `TestGuidEtls`";
                            var res = dbCommand.ExecuteScalar();
                            Assert.Equal(expected, Convert.ToInt32(res));
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.NpgSQL:
                    using (var connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT COUNT(*) FROM \"TestGuidEtls\"";
                            var res = dbCommand.ExecuteScalar();
                            Assert.NotNull(res);
                            Assert.Equal(expected, Convert.ToInt32(res));
                        }
                        connection.Close();
                    }
                    return;
                default:
                    throw new InvalidOperationException(nameof(provider));
            }
        }

        private static void CheckSelectCommand(MigrationProvider provider, Guid guid, string connectionString)
        {
            switch (provider)
            {
                case MigrationProvider.Oracle:
                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT \"Guid\" FROM \"TestGuidEtls\"";
                            using (var r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = new Guid((string)r.GetValue(0));
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MsSQL:
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT \"Guid\" FROM \"TestGuidEtls\"";
                            using (var r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = new Guid((string)r.GetValue(0));
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MySQL:
                    using (var connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT `Guid` FROM `TestGuidEtls`";
                            using (MySqlDataReader r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = new Guid((string)r.GetValue(0));
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.NpgSQL:
                    using (var connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT \"Guid\" FROM \"TestGuidEtls\"";
                            using (var r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = new Guid((string)r.GetValue(0));
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                default:
                    throw new InvalidOperationException(nameof(provider));
            }
        }

        public static Guid GuidFlipEndian(Guid guid)
        {
            var newBytes = new byte[16];
            var oldBytes = guid.ToByteArray();

            for (var i = 8; i < 16; i++)
                newBytes[i] = oldBytes[i];

            newBytes[3] = oldBytes[0];
            newBytes[2] = oldBytes[1];
            newBytes[1] = oldBytes[2];
            newBytes[0] = oldBytes[3];
            newBytes[5] = oldBytes[4];
            newBytes[4] = oldBytes[5];
            newBytes[6] = oldBytes[7];
            newBytes[7] = oldBytes[6];

            return new Guid(newBytes);
        }

        private static void CheckSelectGuidCommand(MigrationProvider provider, Guid guid, string connectionString)
        {
            switch (provider)
            {
                case MigrationProvider.Oracle:
                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT `Guid` FROM `TestGuidEtls`";
                            using (var r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = GuidFlipEndian(new Guid((byte[])r.GetValue(0)));
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MsSQL:
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT \"Guid\" FROM \"TestGuidEtls\"";
                            using (var r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = (Guid)r.GetValue(0);
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.MySQL:
                    using (var connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT `Guid` FROM `TestGuidEtls`";
                            using (var r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = new Guid((byte[])r.GetValue(0));
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                case MigrationProvider.NpgSQL:
                    using (var connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var dbCommand = connection.CreateCommand())
                        {
                            dbCommand.CommandTimeout = 10 * 60;
                            dbCommand.CommandText = "SELECT \"Guid\" FROM \"TestGuidEtls\"";
                            using (var r = dbCommand.ExecuteReader())
                            {
                                r.Read();
                                var g = (Guid)r.GetValue(0);
                                Assert.Equal(guid, g);
                            }
                        }
                        connection.Close();
                    }
                    return;
                default:
                    throw new InvalidOperationException(nameof(provider));
            }
        }
    }
}
