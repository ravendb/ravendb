using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using Lucene.Net.Documents;
using Raven.Bundles.ReplicateToSql.Data;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Document = Lucene.Net.Documents.Document;

namespace Raven.Bundles.ReplicateToSql
{
    public class ReplicateToSqlIndexUpdateTrigger : AbstractIndexUpdateTrigger
    {
        public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
        {
            var document = Database.Get("Raven/ReplicateToSql/"+indexName, null);
            if (document == null)
                return null; // we don't have any reason to replicate anything 

            var destination = document.DataAsJson.JsonDeserialization<ReplicateToSqlDestination>();

            var connectionString = ConfigurationManager.ConnectionStrings[destination.ConnectionStringName];
            if(connectionString == null)
                throw new InvalidOperationException("Could not find a connection string name: " + destination.ConnectionStringName);
            if(connectionString.ProviderName == null)
                throw new InvalidOperationException("Connection string name '"+destination.ConnectionStringName+"' must specify the provider name");

            var providerFactory = DbProviderFactories.GetFactory(connectionString.ProviderName);

            var connection = providerFactory.CreateConnection();
            connection.ConnectionString = connectionString.ConnectionString;
            return new ReplicateToSqlIndexUpdateBatcher(
                connection,
                destination);
        }

        public class ReplicateToSqlIndexUpdateBatcher : AbstractIndexUpdateTriggerBatcher
        {
            private readonly DbConnection connection;
            private readonly ReplicateToSqlDestination destination;
            private DbTransaction tx;

            public ReplicateToSqlIndexUpdateBatcher(DbConnection connection, ReplicateToSqlDestination destination)
            {
                this.connection = connection;
                this.destination = destination;
            }

            private DbConnection Connection
            {
                get
                {
                    if(connection.State != ConnectionState.Open)
                    {
                        connection.Open();
                        tx = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                    }
                    return connection;
                }
            }

            public override void OnIndexEntryCreated(string indexName, string entryKey, Document document)
            {
                using (var cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = tx;
                    var pkParam = cmd.CreateParameter();
                    pkParam.ParameterName = GetParameterName("entryKey");
                    pkParam.Value = entryKey;
                    cmd.Parameters.Add(pkParam);

                    var sb = new StringBuilder("INSERT INTO ")
                        .Append(destination.TableName)
                        .Append(" (")
                        .Append(destination.PrimaryKeyColumnName)
                        .Append(", ");

                    foreach (var mapping in destination.ColumnsMapping)
                    {
                        sb.Append(mapping.Value).Append(", ");
                    }
                    sb.Length = sb.Length - 2;

                    sb.Append(") \r\nVALUES (")
                        .Append(pkParam.ParameterName)
                        .Append(", ");

                    foreach (var mapping in destination.ColumnsMapping)
                    {
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = GetParameterName(mapping.Key);
                        var field = document.GetFieldable(mapping.Key);
                        if (field == null)
                            parameter.Value = DBNull.Value;
                        else if(field is NumericField)
                        {
                            var numField = (NumericField) field;
                            parameter.Value = numField.GetNumericValue();
                        }
                        else
                            parameter.Value = field.StringValue();
                        cmd.Parameters.Add(parameter);
                        sb.Append(parameter.ParameterName).Append(", ");
                    }
                    sb.Length = sb.Length - 2;
                    sb.Append(")");
                    cmd.CommandText = sb.ToString();
                    cmd.ExecuteNonQuery();
                }
            }

            public override void OnIndexEntryDeleted(string indexName, string entryKey)
            {
                using(var cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = tx;
                    var parameter = cmd.CreateParameter();
                    parameter.ParameterName = GetParameterName("entryKey");
                    parameter.Value = entryKey;
                    cmd.Parameters.Add(parameter);
                    cmd.CommandText = string.Format("DELETE FROM {0} WHERE {1} = {2}", destination.TableName, destination.PrimaryKeyColumnName, parameter.ParameterName);

                    cmd.ExecuteNonQuery();
                }
            }

            private string GetParameterName(string paramName)
            {
                if (connection is SqlConnection)
                    return "@" + paramName;
                return ":" + paramName;
            }

            public override void Dispose()
            {
                tx.Commit();
                connection.Dispose();
            }
        }
    }
}