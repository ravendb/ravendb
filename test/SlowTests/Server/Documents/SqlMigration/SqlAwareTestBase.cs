using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using FastTests;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using SlowTests.Server.Documents.ETL.SQL;
using Tests.Infrastructure;
using DisposableAction = Raven.Client.Util.DisposableAction;

namespace SlowTests.Server.Documents.SqlMigration
{
    public abstract class SqlAwareTestBase : RavenTestBase
    {
        protected void ApplyDefaultColumnNamesMapping(DatabaseSchema dbSchema, MigrationSettings settings, bool binaryToAttachment = false)
        {
            foreach (var collection in settings.Collections)
            {
                ApplyDefaultColumnNamesMapping(dbSchema, collection, binaryToAttachment);
            }
        }

        protected void ApplyDefaultColumnNamesMapping(DatabaseSchema dbSchema, AbstractCollection collection, bool binaryToAttachment)
        {
            var tableSchema = dbSchema.Tables.First(x => x.Schema == collection.SourceTableSchema && x.TableName == collection.SourceTableName);

            var specialColumns = dbSchema.FindSpecialColumns(collection.SourceTableSchema, collection.SourceTableName);

            collection.ColumnsMapping = tableSchema.Columns
                .Where(x => specialColumns.Contains(x.Name) == false && (binaryToAttachment ? x.Type != ColumnType.Binary : true))
                .Select(c => (c.Name, c.Name.First().ToString().ToUpper() + c.Name.Substring(1)))
                .ToDictionary(x => x.Name, x => x.Item2);
            
            collection.AttachmentNameMapping = tableSchema.Columns
                .Where(x => binaryToAttachment ? x.Type == ColumnType.Binary : false)
                .Select(c => (c.Name, c.Name.First().ToString().ToUpper() + c.Name.Substring(1)))
                .ToDictionary(x => x.Name, x => x.Item2);

            if (collection is CollectionWithReferences collectionWithRefs)
            {
                if (collectionWithRefs.LinkedCollections != null)
                {
                    foreach (var linkedCollection in collectionWithRefs.LinkedCollections)
                    {
                        ApplyDefaultColumnNamesMapping(dbSchema, linkedCollection, binaryToAttachment);
                    }
                }

                if (collectionWithRefs.NestedCollections != null)
                {
                    foreach (var embeddedCollection in collectionWithRefs.NestedCollections)
                    {
                        ApplyDefaultColumnNamesMapping(dbSchema, embeddedCollection, binaryToAttachment);
                    }
                }
            }
        }

        protected void ExecuteSqlQuery(MigrationProvider provider, string connectionString, string query)
        {
            switch (provider)
            {
                case MigrationProvider.MySQL:
                {
                    using (var connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var cmd = new MySqlCommand(query, connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    break;
                }
                case MigrationProvider.MsSQL:
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    break;
                case MigrationProvider.NpgSQL:
                    using (var connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var cmd = new NpgsqlCommand(query, connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    break;
                case MigrationProvider.OracleClient:
                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();
                        using (var cmd = new OracleCommand(query, connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    break;
                default:
                    throw new InvalidOperationException("Unable to run query. Unsupported provider: " + provider);
            }
        }

        protected DisposableAction WithSqlDatabase(MigrationProvider provider, out string connectionString, out string schemaName, string dataSet = "northwind", bool includeData = true)
        {
            switch (provider)
            {
                case MigrationProvider.MySQL:
                    return WithMySqlDatabase(out connectionString, out schemaName, dataSet, includeData);
                case MigrationProvider.MsSQL:
                    schemaName = "dbo";
                    return WithMsSqlDatabase(out connectionString, out string databaseName, dataSet, includeData);
                case MigrationProvider.NpgSQL:
                    schemaName = "public";
                    return WithNpgSqlDatabase(out connectionString, out string dbName, dataSet, includeData);
                case MigrationProvider.OracleClient:
                    return WithOracleDatabase(out connectionString, out schemaName, dataSet, includeData);
                default:
                    throw new InvalidOperationException("Unhandled provider: " + provider);
            }
        }

        private DisposableAction WithMsSqlDatabase(out string connectionString, out string databaseName, string dataSet, bool includeData = true)
        {
            databaseName = "SqlTest_" + Guid.NewGuid();
            connectionString = SqlEtlTests.MasterDatabaseConnection.Value + $";Initial Catalog={databaseName}";

            using (var connection = new SqlConnection(SqlEtlTests.MasterDatabaseConnection.Value))
            {
                connection.Open();

                using (var dbCommand = connection.CreateCommand())
                {
                    var createDatabaseQuery = "USE master IF EXISTS(select * from sys.databases where name= '{0}') DROP DATABASE[{0}] CREATE DATABASE[{0}]";
                    dbCommand.CommandText = string.Format(createDatabaseQuery, databaseName);
                    dbCommand.ExecuteNonQuery();
                }
            }

            using (var dbConnection = new SqlConnection(connectionString))
            {
                dbConnection.Open();

                var assembly = Assembly.GetExecutingAssembly();

                var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.mssql." + dataSet + ".create.sql"));
                var commands = textStreamReader.ReadToEnd().Split(" GO ");
                
                foreach (var command in commands)
                {
                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        dbCommand.CommandText = command;
                        dbCommand.ExecuteNonQuery();
                    }
                }
                

                if (includeData)
                {
                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        var dataStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.mssql." + dataSet + ".insert.sql"));
                        dbCommand.CommandText = dataStreamReader.ReadToEnd();
                        dbCommand.ExecuteNonQuery();
                    }
                }
            }
            
            var dbName = databaseName;

            return new DisposableAction(() =>
            {
                using (var con = new SqlConnection(SqlEtlTests.MasterDatabaseConnection.Value))
                {
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        var dropDatabaseQuery = "IF EXISTS(select * from sys.databases where name= '{0}') " +
                                                "ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; " +
                                                "IF EXISTS(select * from sys.databases where name= '{0}') DROP DATABASE [{0}]";
                        dbCommand.CommandText = string.Format(dropDatabaseQuery, dbName);

                        dbCommand.ExecuteNonQuery();
                    }
                }
            });
        }

        protected DisposableAction WithMySqlDatabase(out string connectionString, out string databaseName, string dataSet, bool includeData = true)
        {
            databaseName = "sql_test_" + Guid.NewGuid();
            var rawConnectionString = MySqlTests.MySqlDatabaseConnection.Value;
            connectionString = rawConnectionString + $";database=\"{databaseName}\"";

            using (var connection = new MySqlConnection(rawConnectionString))
            {
                connection.Open();

                using (var dbCommand = connection.CreateCommand())
                {
                    var createDatabaseQuery = "CREATE DATABASE `{0}`";
                    dbCommand.CommandText = string.Format(createDatabaseQuery, databaseName);
                    dbCommand.ExecuteNonQuery();
                }
            }

            using (var dbConnection = new MySqlConnection(connectionString))
            {
                dbConnection.Open();

                var assembly = Assembly.GetExecutingAssembly();

                using (var dbCommand = dbConnection.CreateCommand())
                {
                    var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.mysql." + dataSet + ".create.sql"));
                    dbCommand.CommandText = textStreamReader.ReadToEnd();
                    dbCommand.ExecuteNonQuery();
                }

                if (includeData)
                {
                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.mysql." + dataSet + ".insert.sql"));
                        dbCommand.CommandText = textStreamReader.ReadToEnd();
                        dbCommand.ExecuteNonQuery();
                    }
                }
            }

            string dbName = databaseName;
            return new DisposableAction(() =>
            {
                using (var con = new MySqlConnection(rawConnectionString))
                {
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        var dropDatabaseQuery = "DROP DATABASE `{0}`";
                        dbCommand.CommandText = string.Format(dropDatabaseQuery, dbName);

                        dbCommand.ExecuteNonQuery();
                    }
                }
            });
        }

        protected DisposableAction WithNpgSqlDatabase(out string connectionString, out string databaseName, string dataSet, bool includeData = true)
        {
            databaseName = "npgSql_test_" + Guid.NewGuid();
            var rawConnectionString = NpgSqlTests.NpgSqlDatabaseConnection.Value;
            connectionString = rawConnectionString + $";Database=\"{databaseName}\"";

            using (var connection = new NpgsqlConnection(rawConnectionString))
            {
                connection.Open();
                using (var dbCommand = connection.CreateCommand())
                {
                    const string createDatabaseQuery = "CREATE DATABASE \"{0}\"";
                    dbCommand.CommandText = string.Format(createDatabaseQuery, databaseName);
                    dbCommand.ExecuteNonQuery();
                }
                connection.Close();
            }

            using (var dbConnection = new NpgsqlConnection(connectionString))
            {
                // ConnectionString with DB
                dbConnection.Open();
                var assembly = Assembly.GetExecutingAssembly();

                using (var dbCommand = dbConnection.CreateCommand())
                {
                    var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.npgsql." + dataSet + ".create.sql"));
                    dbCommand.CommandText = textStreamReader.ReadToEnd();
                    dbCommand.ExecuteNonQuery();
                }

                if (includeData)
                {
                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        var dataStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.npgsql." + dataSet + ".insert.sql"));
                        dbCommand.CommandText = dataStreamReader.ReadToEnd();
                        dbCommand.ExecuteNonQuery();
                    }
                }
            }

            var dbName = databaseName;
            return new DisposableAction(() =>
            {
                using (var con = new NpgsqlConnection(rawConnectionString))
                {
                    con.Open();
                    using (var dbCommand = con.CreateCommand())
                    {
                        const string dropDatabaseQuery = "DROP DATABASE IF EXISTS \"{0}\"";
                        dbCommand.CommandText = string.Format(dropDatabaseQuery, dbName);
                        dbCommand.ExecuteNonQuery();
                    }
                }
            });
        }

        protected DisposableAction WithOracleDatabase(out string connectionString, out string databaseName, string dataSet, bool includeData = true)
        {
            databaseName = "C##" + Guid.NewGuid();
            var pass = "pass";
            var adminConnectionString = OracleClientTests.OracleClientDatabaseConnection.Value;

            using (var connection = new OracleConnection(adminConnectionString))
            {
                connection.Open();
                using (var dbCommand = connection.CreateCommand())
                {
                    List<string> cmdList = new List<string>();
                    cmdList.Add($"CREATE USER \"{databaseName}\" IDENTIFIED BY {pass}");
                    cmdList.Add($"GRANT CONNECT, RESOURCE, DBA TO \"{databaseName}\"");
                    cmdList.Add($"GRANT CREATE SESSION, create table, create sequence, create trigger TO \"{databaseName}\"");
                    cmdList.Add($"GRANT UNLIMITED TABLESPACE TO \"{databaseName}\"");


                    foreach (var cmd in cmdList)
                    {
                        dbCommand.CommandText = cmd;
                        dbCommand.ExecuteNonQuery();
                    }
                }
                connection.Close();
            }
            var userConnectionString = OracleClientTests.OracleDataSource + $"USER ID=\"{databaseName}\";password={pass};Pooling=false";
            connectionString = userConnectionString;
            using (var dbConnection = new OracleConnection(connectionString))
            {
                // ConnectionString with DB
                dbConnection.Open();
                var assembly = Assembly.GetExecutingAssembly();
                using (var dbCommand = dbConnection.CreateCommand())
                {
                    using (var reader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.oraclesql." + dataSet + ".create.sql")))
                    {
                        while (reader.Peek() >= 0)
                        {
                            dbCommand.CommandText = reader.ReadLine();
                            dbCommand.ExecuteNonQuery();
                        }
                    }
                }
                if (includeData)
                {
                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        using (var reader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.oraclesql." + dataSet + ".insert.sql")))
                        {
                            while (reader.Peek() >= 0)
                            {
                                dbCommand.CommandText = reader.ReadLine();
                                dbCommand.ExecuteNonQuery();
                            }
                        }
                    }
                }
                dbConnection.Close();
            }

            var dbName = databaseName;
            return new DisposableAction(() =>
            {
                using (var con = new OracleConnection(adminConnectionString))
                {
                    con.Open();
                    using (var dbCommand = con.CreateCommand())
                    {
                        var dropDatabaseQuery = $"DROP USER \"{dbName}\" CASCADE";
                        dbCommand.CommandText = dropDatabaseQuery;
                        dbCommand.ExecuteNonQuery();
                    }
                }
            });
        }
    }
}
