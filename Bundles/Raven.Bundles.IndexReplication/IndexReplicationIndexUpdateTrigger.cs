//-----------------------------------------------------------------------
// <copyright file="IndexReplicationIndexUpdateTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Documents;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.IndexReplication.Data;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Document = Lucene.Net.Documents.Document;

namespace Raven.Bundles.IndexReplication
{
	public class IndexReplicationIndexUpdateTrigger : AbstractIndexUpdateTrigger
	{
		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(int indexId)
		{
		    Index indexInstance = this.Database.IndexStorage.GetIndexInstance(indexId);
		    if (indexInstance == null)
		        return null;
		    var document = Database.Get("Raven/IndexReplication/" + indexInstance.PublicName, null);
			if (document == null)
				return null; // we don't have any reason to replicate anything 

			var destination = document.DataAsJson.JsonDeserialization<IndexReplicationDestination>();

			var connectionString = ConfigurationManager.ConnectionStrings[destination.ConnectionStringName];
			if (connectionString == null)
				throw new InvalidOperationException("Could not find a connection string name: " + destination.ConnectionStringName);
			if (connectionString.ProviderName == null)
				throw new InvalidOperationException("Connection string name '" + destination.ConnectionStringName + "' must specify the provider name");

			var providerFactory = DbProviderFactories.GetFactory(connectionString.ProviderName);

			return new ReplicateToSqlIndexUpdateBatcher(
				providerFactory,
				connectionString.ConnectionString,
				destination);
		}

		public class ReplicateToSqlIndexUpdateBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly DbProviderFactory _providerFactory;
			private readonly DbCommandBuilder _commandBuilder;
			private readonly string _connectionString;
			private readonly IndexReplicationDestination destination;
			private static readonly Regex datePattern = new Regex(@"\d{17}", RegexOptions.Compiled);

			private readonly ConcurrentQueue<IDbCommand> commands = new ConcurrentQueue<IDbCommand>();

			public ReplicateToSqlIndexUpdateBatcher(
				DbProviderFactory providerFactory,
				string connectionString,
				IndexReplicationDestination destination)
			{
				_providerFactory = providerFactory;
				_commandBuilder = providerFactory.CreateCommandBuilder();
				_connectionString = connectionString;
				this.destination = destination;
			}

			public override void OnIndexEntryCreated(string entryKey, Document document)
			{
				var cmd = _providerFactory.CreateCommand();
				var pkParam = cmd.CreateParameter();
				pkParam.ParameterName = GetParameterName("entryKey");
				pkParam.Value = entryKey;
				cmd.Parameters.Add(pkParam);

				var sb = new StringBuilder("INSERT INTO ")
					.Append(_commandBuilder.QuoteIdentifier(destination.TableName))
					.Append(" (")
					.Append(_commandBuilder.QuoteIdentifier(destination.PrimaryKeyColumnName))
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

					if (field == null || field.StringValue == Constants.NullValue)
						parameter.Value = DBNull.Value;
					else if (field.StringValue == Constants.EmptyString)
						parameter.Value = "";
					else if (field.StringValue.Equals("False", StringComparison.InvariantCultureIgnoreCase))
					{
						parameter.Value = false;
					}
					else if (field.StringValue.Equals("True", StringComparison.InvariantCultureIgnoreCase))
					{
						parameter.Value = true;
					}
					else if (field is NumericField)
					{
						var numField = (NumericField) field;
						parameter.Value = numField.NumericValue;
					}
					else
					{
						var stringValue = field.StringValue;
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

				commands.Enqueue(cmd);
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
				var cmd = _providerFactory.CreateCommand();
				var parameter = cmd.CreateParameter();
				parameter.ParameterName = GetParameterName("entryKey");
				parameter.Value = entryKey;
				cmd.Parameters.Add(parameter);
				cmd.CommandText = string.Format("DELETE FROM {0} WHERE {1} = {2}",
				                                _commandBuilder.QuoteIdentifier(destination.TableName),
				                                _commandBuilder.QuoteIdentifier(destination.PrimaryKeyColumnName),
				                                parameter.ParameterName);

				commands.Enqueue(cmd);
			}

			private static readonly Func<DbCommandBuilder, string, string> GetParameterNameFromBuilder =
				(Func<DbCommandBuilder, string, string>)
				Delegate.CreateDelegate(typeof(Func<DbCommandBuilder, string, string>),
				                        typeof(DbCommandBuilder).GetMethod("GetParameterName",
				                                                           BindingFlags.Instance | BindingFlags.NonPublic, Type.DefaultBinder,
				                                                           new[] { typeof(string) }, null));

			private string GetParameterName(string paramName)
			{
				switch (_providerFactory.GetType().Name)
				{
					case "SqlClientFactory":
					case "MySqlClientFactory":
						return "@" + paramName;

					case "OracleClientFactory":
					case "NpgsqlFactory":
						return ":" + paramName;

					default:
						// If we don't know, try to get it from the CommandBuilder.
						return GetParameterNameFromBuilder(_commandBuilder, paramName);
				}
			}

			public override void Dispose()
			{
				if (_commandBuilder != null)
					_commandBuilder.Dispose();

				if (commands.Count == 0)
					return;

				using (var con = _providerFactory.CreateConnection())
				{
					con.ConnectionString = _connectionString;
					con.Open();

					using (var tx = con.BeginTransaction())
					{
						IDbCommand result;
						while (commands.TryDequeue(out result))
						{
							result.Connection = con;
							result.Transaction = tx;
							result.ExecuteNonQuery();
						}

						tx.Commit();
					}
				}
			}
		}
	}
}
