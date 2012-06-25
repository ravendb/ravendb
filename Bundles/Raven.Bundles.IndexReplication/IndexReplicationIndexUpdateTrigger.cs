//-----------------------------------------------------------------------
// <copyright file="IndexReplicationIndexUpdateTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Documents;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.IndexReplication.Data;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Document = Lucene.Net.Documents.Document;

namespace Raven.Bundles.IndexReplication
{
	public class IndexReplicationIndexUpdateTrigger : AbstractIndexUpdateTrigger
	{
		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
		{
			var document = Database.Get("Raven/IndexReplication/" + indexName, null);
			if (document == null)
				return null; // we don't have any reason to replicate anything 

			var destination = document.DataAsJson.JsonDeserialization<IndexReplicationDestination>();

			var connectionString = ConfigurationManager.ConnectionStrings[destination.ConnectionStringName];
			if (connectionString == null)
				throw new InvalidOperationException("Could not find a connection string name: " + destination.ConnectionStringName);
			if (connectionString.ProviderName == null)
				throw new InvalidOperationException("Connection string name '" + destination.ConnectionStringName + "' must specify the provider name");

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
			private readonly IndexReplicationDestination destination;
			private DbTransaction tx;
			private static Regex datePattern = new Regex(@"\d{17}", RegexOptions.Compiled);

			public ReplicateToSqlIndexUpdateBatcher(DbConnection connection, IndexReplicationDestination destination)
			{
				this.connection = connection;
				this.destination = destination;
			}

			private DbConnection Connection
			{
				get
				{
					if (connection.State != ConnectionState.Open)
					{
						connection.Open();
						tx = connection.BeginTransaction(IsolationLevel.ReadCommitted);
					}
					return connection;
				}
			}

			public override void OnIndexEntryCreated(string entryKey, Document document)
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

						var numericfield = document.GetFieldable(String.Concat(mapping.Key, "_Range"));
						if (numericfield != null)
							field = numericfield;

						if (field == null || field.StringValue() == Constants.NullValue)
							parameter.Value = DBNull.Value;
						else if (field is NumericField)
						{
							var numField = (NumericField)field;
							parameter.Value = numField.GetNumericValue();
						}
						else
						{
							var stringValue = field.StringValue();
							if (datePattern.IsMatch(stringValue))
							{
								try
								{
									parameter.Value = DateTools.StringToDate(stringValue);
								}
								catch
								{
									parameter.Value = stringValue;
								}
							}
							else
							{
								DateTime time;
								if (DateTime.TryParseExact(stringValue, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
														   DateTimeStyles.None, out time))
								{
									parameter.Value = time;
								}
								else
								{
									parameter.Value = stringValue;
								}
							}
						}

						cmd.Parameters.Add(parameter);
						sb.Append(parameter.ParameterName).Append(", ");
					}
					sb.Length = sb.Length - 2;
					sb.Append(")");
					cmd.CommandText = sb.ToString();
					cmd.ExecuteNonQuery();
				}
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
				using (var cmd = Connection.CreateCommand())
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