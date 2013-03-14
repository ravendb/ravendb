using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Indexing;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	public class RelationalDatabaseWriter : IDisposable
	{
		private readonly DocumentDatabase database;
		private readonly SqlReplicationConfig cfg;
		private readonly DbProviderFactory providerFactory;
		private readonly ConversionScriptResult scriptResult;
		private readonly SqlReplicationStatistics replicationStatistics;
		private readonly DbCommandBuilder commandBuilder;
		private readonly DbConnection connection;
		private readonly DbTransaction tx;

		private static ILog log = LogManager.GetCurrentClassLogger();

		bool hadErrors;

		public RelationalDatabaseWriter(
			DocumentDatabase database,
			SqlReplicationConfig cfg, DbProviderFactory providerFactory, ConversionScriptResult scriptResult, SqlReplicationStatistics replicationStatistics)
		{
			this.database = database;
			this.cfg = cfg;
			this.providerFactory = providerFactory;
			this.scriptResult = scriptResult;
			this.replicationStatistics = replicationStatistics;

			commandBuilder = providerFactory.CreateCommandBuilder();
			connection = providerFactory.CreateConnection();

			Debug.Assert(connection != null);
			Debug.Assert(commandBuilder != null);

			connection.ConnectionString = cfg.ConnectionString;

			try
			{
				connection.Open();
			}
			catch (Exception e)
			{
				database.AddAlert(new Alert
				{
					AlertLevel = AlertLevel.Error,
					CreatedAt = SystemTime.UtcNow,
					Exception = e.ToString(),
					Title = "Sql Replication could not open connection",
					Message = "Sql Replication could not open connection to " + connection.ConnectionString,
					UniqueKey = "Sql Replication Connection Error: " + connection.ConnectionString
				});
				throw;
			}

			tx = connection.BeginTransaction();
		}

		public bool Execute()
		{
			foreach (var tableName in scriptResult.TablesInOrder)
			{
				var dataForTable = scriptResult.Data[tableName];

				if (dataForTable.Count == 0)
					continue; // shouldn't happen, but anyway...

				// first, delete all the rows that might already exist there
				DeleteItems(tableName, dataForTable[0].PkName, dataForTable.Select(x => x.DocumentId).ToList());

				InsertItems(tableName, dataForTable);
			}
			tx.Commit();

			return hadErrors == false;
		}

		private void InsertItems(string tableName, List<ItemToReplicate> dataForTable)
		{
			foreach (var itemToReplicate in dataForTable)
			{
				using (var cmd = connection.CreateCommand())
				{
					cmd.Transaction = tx;

					var sb = new StringBuilder("INSERT INTO ")
						.Append(commandBuilder.QuoteIdentifier(tableName))
						.Append(" (")
						.Append(commandBuilder.QuoteIdentifier(itemToReplicate.PkName))
						.Append(", ");
					foreach (var column in itemToReplicate.Columns)
					{
						if (column.Key == itemToReplicate.PkName)
							continue;
						sb.Append(commandBuilder.QuoteIdentifier(column.Key)).Append(", ");
					}
					sb.Length = sb.Length - 2;

					var pkParam = cmd.CreateParameter();
					pkParam.ParameterName = GetParameterName(providerFactory, commandBuilder, itemToReplicate.PkName);
					pkParam.Value = itemToReplicate.DocumentId;
					cmd.Parameters.Add(pkParam);

					sb.Append(") \r\nVALUES (")
					  .Append(GetParameterName(providerFactory, commandBuilder, itemToReplicate.PkName))
					  .Append(", ");

					foreach (var column in itemToReplicate.Columns)
					{
						if (column.Key == itemToReplicate.PkName)
							continue;
						var colParam = cmd.CreateParameter();
						colParam.ParameterName = column.Key;
						SetParamValue(colParam, column.Value);
						cmd.Parameters.Add(colParam);
						sb.Append(GetParameterName(providerFactory, commandBuilder, column.Key)).Append(", ");
					}
					sb.Length = sb.Length - 2;
					sb.Append(")");
					cmd.CommandText = sb.ToString();
					try
					{
						cmd.ExecuteNonQuery();
					}
					catch (Exception e)
					{
						log.WarnException(
							"Failure to replicate changes to relational database for: " + cfg.Name + ", will continue trying." +
							Environment.NewLine + cmd.CommandText, e);
						replicationStatistics.RecordWriteError(e, database);
						hadErrors = true;
					}
				}
			}
		}

		public void DeleteItems(string tableName, string pkName, List<string> identifiers)
		{
			const int maxParams = 1000;
			using (var cmd = connection.CreateCommand())
			{
				cmd.Transaction = tx;
				for (int i = 0; i < identifiers.Count; i += maxParams)
				{
					cmd.Parameters.Clear();
					var sb = new StringBuilder("DELETE FROM ")
						.Append(commandBuilder.QuoteIdentifier(tableName))
						.Append(" WHERE ")
						.Append(commandBuilder.QuoteIdentifier(pkName))
						.Append(" IN (");

					for (int j = i; j < maxParams; j++)
					{
						var dbParameter = cmd.CreateParameter();
						dbParameter.ParameterName = GetParameterName(providerFactory, commandBuilder, "p" + i);
						dbParameter.Value = identifiers[j];

						if (i != j)
							sb.Append(", ");

						sb.Append(dbParameter.ParameterName);
					}
					sb.Append(")");

					cmd.CommandText = sb.ToString();

					try
					{
						cmd.ExecuteNonQuery();
					}
					catch (Exception e)
					{
						log.WarnException(
							"Failure to replicate changes to relational database for: " + cfg.Name + ", will continue trying." +
							Environment.NewLine + cmd.CommandText, e);
						replicationStatistics.RecordWriteError(e, database);
						hadErrors = true;
					}
				}
			}
		}

		private static string GetParameterName(DbProviderFactory providerFactory, DbCommandBuilder commandBuilder, string paramName)
		{
			switch (providerFactory.GetType().Name)
			{
				case "SqlClientFactory":
				case "MySqlClientFactory":
					return "@" + paramName;

				case "OracleClientFactory":
				case "NpgsqlFactory":
					return ":" + paramName;

				default:
					// If we don't know, try to get it from the CommandBuilder.
					return getParameterNameFromBuilder(commandBuilder, paramName);
			}
		}

		private static readonly Func<DbCommandBuilder, string, string> getParameterNameFromBuilder =
			(Func<DbCommandBuilder, string, string>)
			Delegate.CreateDelegate(typeof(Func<DbCommandBuilder, string, string>),
									typeof(DbCommandBuilder).GetMethod("GetParameterName",
																	   BindingFlags.Instance | BindingFlags.NonPublic, Type.DefaultBinder,
																	   new[] { typeof(string) }, null));


		private static void SetParamValue(DbParameter colParam, RavenJToken val)
		{
			if (val == null)
				colParam.Value = DBNull.Value;
			else
			{
				switch (val.Type)
				{
					case JTokenType.None:
					case JTokenType.Object:
					case JTokenType.Uri:
					case JTokenType.Raw:
					case JTokenType.Array:
						colParam.Value = val.Value<string>();
						return;
					case JTokenType.String:
						var value = val.Value<string>();
						if (value.Length > 0)
						{
							if (char.IsDigit(value[0]))
							{
								DateTime dateTime;
								if (DateTime.TryParseExact(value, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture,
														   DateTimeStyles.RoundtripKind, out dateTime))
								{
									colParam.Value = dateTime;
									return;
								}
								DateTimeOffset dateTimeOffset;
								if (DateTimeOffset.TryParseExact(value, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
																 DateTimeStyles.RoundtripKind, out dateTimeOffset))
								{
									colParam.Value = dateTimeOffset;
									return;
								}
							}
						}
						colParam.Value = value;
						return;
					case JTokenType.Integer:
					case JTokenType.Date:
					case JTokenType.Bytes:
					case JTokenType.Guid:
					case JTokenType.Boolean:
					case JTokenType.TimeSpan:
					case JTokenType.Float:
						colParam.Value = val.Value<object>();
						return;
					case JTokenType.Null:
					case JTokenType.Undefined:
						colParam.Value = DBNull.Value;
						return;
					default:
						throw new InvalidOperationException("Cannot understand how to save " + val.Type + " for " + colParam.ParameterName);
				}
			}
		}


		public void Dispose()
		{
			tx.Dispose();
			commandBuilder.Dispose();
			connection.Dispose();
		}
	}
}