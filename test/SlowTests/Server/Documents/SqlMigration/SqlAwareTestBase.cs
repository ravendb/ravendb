﻿using System;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using FastTests;
using MySql.Data.MySqlClient;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using SlowTests.Server.Documents.ETL.SQL;
using Tests.Infrastructure;
using Voron.Util;
using DisposableAction = Raven.Client.Util.DisposableAction;

namespace SlowTests.Server.Documents.SqlMigration
{
    public abstract class SqlAwareTestBase : RavenTestBase
    {
        protected void ApplyDefaultColumnNamesMapping(DatabaseSchema dbSchema, MigrationSettings settings)
        {
            foreach (var collection in settings.Collections)
            {
                ApplyDefaultColumnNamesMapping(dbSchema, collection, settings.BinaryToAttachment);
            }
        }

        protected void ApplyDefaultColumnNamesMapping(DatabaseSchema dbSchema, AbstractCollection collection, bool binaryToAttachment)
        {
            var tableSchema = dbSchema.Tables.First(x => x.Schema == collection.SourceTableSchema && x.TableName == collection.SourceTableName);

            var specialColumns = dbSchema.FindSpecialColumns(collection.SourceTableSchema, collection.SourceTableName);
            var attachmentColumns = tableSchema.GetAttachmentColumns(binaryToAttachment);

            var mapping = tableSchema.Columns
                .Where(x => specialColumns.Contains(x.Name) == false && attachmentColumns.Contains(x.Name) == false)
                .Select(c => (c.Name, c.Name.First().ToString().ToUpper() + c.Name.Substring(1)))
                .ToDictionary(x => x.Name, x => x.Item2);

            collection.ColumnsMapping = mapping;

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
    }
}
