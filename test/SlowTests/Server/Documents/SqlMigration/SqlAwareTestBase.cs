using System;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using FastTests;
using MySql.Data.MySqlClient;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.ETL.SQL;
using Voron.Util;
using DisposableAction = Raven.Client.Util.DisposableAction;

namespace SlowTests.Server.Documents.SqlMigration
{
    public abstract class SqlAwareTestBase : RavenTestBase
    {
        public static readonly Lazy<string> MySqlDatabaseConnection = new Lazy<string>(() =>
        {
            //TODO: use ENV variable to allow different values
            var local = @"server=127.0.0.1;uid=root;pwd=";
            using (var con = new MySqlConnection(local))
            {
                con.Open();
            }
            return local;
        });
        
        protected DisposableAction WithSqlDatabase(MigrationProvider provider, out string connectionString, string dataSet = "northwind", bool includeData = true)
        {
            switch (provider)
            {
                case MigrationProvider.MySQL:
                    return WithMySqlDatabase(out connectionString, dataSet, includeData);
                case MigrationProvider.MsSQL:
                    return WithMsSqlDatabase(out connectionString, dataSet, includeData);
                default:
                    throw new InvalidOperationException("Unhandled provider: " + provider);
            }
        }
        
        private DisposableAction WithMsSqlDatabase(out string connectionString, string dataSet, bool includeData = true)
        {
            var databaseName = "SqlTest_" + Guid.NewGuid();
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

                using (var dbCommand = dbConnection.CreateCommand())
                {
                    var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.mssql." + dataSet + ".create.sql"));
                    dbCommand.CommandText = textStreamReader.ReadToEnd();
                    dbCommand.ExecuteNonQuery();
                }

                if (includeData)
                {
                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.mssql." + dataSet + ".insert.sql"));
                        dbCommand.CommandText = textStreamReader.ReadToEnd();
                        dbCommand.ExecuteNonQuery();
                    }
                }
            }

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
                        dbCommand.CommandText = string.Format(dropDatabaseQuery, databaseName);

                        dbCommand.ExecuteNonQuery();
                    }
                }
            });
        }

        protected DisposableAction WithMySqlDatabase(out string connectionString, string dataSet, bool includeData = true)
        {
            var databaseName = "SqlTest_" + Guid.NewGuid();
            var rawConnectionString = MySqlDatabaseConnection.Value;
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

            return new DisposableAction(() =>
            {
                using (var con = new MySqlConnection(rawConnectionString))
                {
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        var dropDatabaseQuery = "DROP DATABASE `{0}`";
                        dbCommand.CommandText = string.Format(dropDatabaseQuery, databaseName);

                        dbCommand.ExecuteNonQuery();
                    }
                }
            });
        }
    }
}
