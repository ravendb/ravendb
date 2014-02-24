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
using Raven.Database.Extensions;
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
		private readonly SqlReplicationStatistics replicationStatistics;
		private readonly DbCommandBuilder commandBuilder;
		private readonly DbConnection connection;
		private readonly DbTransaction tx;
		private readonly List<Func<DbParameter, String, Boolean>> stringParserList;

		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		bool hadErrors;

		public RelationalDatabaseWriter( DocumentDatabase database, SqlReplicationConfig cfg, SqlReplicationStatistics replicationStatistics)
		{
			this.database = database;
			this.cfg = cfg;
			this.replicationStatistics = replicationStatistics;

			providerFactory = GetDbProviderFactory(cfg);

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

			stringParserList = new List<Func<DbParameter, string, bool>> { 
				(colParam, value) => {
					if( char.IsDigit( value[ 0 ] ) ) {
							DateTime dateTime;
							if (DateTime.TryParseExact(value, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture,
														DateTimeStyles.RoundtripKind, out dateTime))
							{
								switch( providerFactory.GetType( ).Name ) {
									case "MySqlClientFactory":
										colParam.Value = dateTime.ToString( "yyyy-MM-dd HH:mm:ss.ffffff" );
										break;
									default:
										colParam.Value = dateTime;
										break;
								}
								return true;
							}
					}
					return false;
				},
				(colParam, value) => {
					if( char.IsDigit( value[ 0 ] ) ) {
						DateTimeOffset dateTimeOffset;
						if( DateTimeOffset.TryParseExact( value, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
														 DateTimeStyles.RoundtripKind, out dateTimeOffset ) ) {
							switch( providerFactory.GetType( ).Name ) {
								case "MySqlClientFactory":
									colParam.Value = dateTimeOffset.ToUniversalTime().ToString( "yyyy-MM-dd HH:mm:ss.ffffff" );
									break;
								default:
									colParam.Value = dateTimeOffset;
									break;
							}
							return true;
						}
					}
					return false;
				}
			};
		}

		public bool Execute(ConversionScriptResult scriptResult)
		{
			var identifiers = scriptResult.Data.SelectMany(x => x.Value).Select(x => x.DocumentId).Distinct().ToList();
			foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
			{
				// first, delete all the rows that might already exist there
				DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, cfg.ParameterizeDeletesDisabled,
										identifiers);
			}

			foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
			{
				List<ItemToReplicate> dataForTable;
				if (scriptResult.Data.TryGetValue(sqlReplicationTable.TableName, out dataForTable) == false)
					continue;

				InsertItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, dataForTable);
			}

			Commit();

			return hadErrors == false;
		}

		public bool Commit()
		{
			tx.Commit();
			return true;
		}

		private void InsertItems(string tableName, string pkName, List<ItemToReplicate> dataForTable)
		{
			foreach (var itemToReplicate in dataForTable)
			{
				using (var cmd = connection.CreateCommand())
				{
					cmd.Transaction = tx;

					database.WorkContext.CancellationToken.ThrowIfCancellationRequested();

					var sb = new StringBuilder("INSERT INTO ")
						.Append(commandBuilder.QuoteIdentifier(tableName))
						.Append(" (")
						.Append(commandBuilder.QuoteIdentifier(pkName))
						.Append(", ");
					foreach (var column in itemToReplicate.Columns)
					{
						if (column.Key == pkName)
							continue;
						sb.Append(commandBuilder.QuoteIdentifier(column.Key)).Append(", ");
					}
					sb.Length = sb.Length - 2;

					var pkParam = cmd.CreateParameter();
					pkParam.ParameterName = GetParameterName(providerFactory, commandBuilder, pkName);
					pkParam.Value = itemToReplicate.DocumentId;
					cmd.Parameters.Add(pkParam);

					sb.Append(") \r\nVALUES (")
						.Append(GetParameterName(providerFactory, commandBuilder, pkName))
						.Append(", ");

					foreach (var column in itemToReplicate.Columns)
					{
						if (column.Key == pkName)
							continue;
						var colParam = cmd.CreateParameter();
						colParam.ParameterName = column.Key;
						SetParamValue( colParam, column.Value, stringParserList );
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
							"Failure to replicate changes to relational database for: " + cfg.Name + " (doc: "+  itemToReplicate.DocumentId +" ), will continue trying." +
							Environment.NewLine + cmd.CommandText, e);
						replicationStatistics.RecordWriteError(e, database);
						hadErrors = true;
					}
				}
			}
		}

		public void DeleteItems(string tableName, string pkName, bool doNotParameterize, List<string> identifiers)
		{
			const int maxParams = 1000;
			using (var cmd = connection.CreateCommand())
			{
				cmd.Transaction = tx;
				database.WorkContext.CancellationToken.ThrowIfCancellationRequested();
				for (int i = 0; i < identifiers.Count; i += maxParams)
				{
					cmd.Parameters.Clear();
					var sb = new StringBuilder("DELETE FROM ")
						.Append(commandBuilder.QuoteIdentifier(tableName))
						.Append(" WHERE ")
						.Append(commandBuilder.QuoteIdentifier(pkName))
						.Append(" IN (");

					for (int j = i; j < Math.Min(i + maxParams, identifiers.Count); j++)
					{
						if (i != j)
							sb.Append(", ");
						if (doNotParameterize == false)
						{
							var dbParameter = cmd.CreateParameter();
							dbParameter.ParameterName = GetParameterName(providerFactory, commandBuilder, "p" + j);
							dbParameter.Value = identifiers[j];
							cmd.Parameters.Add(dbParameter);
							sb.Append(dbParameter.ParameterName);
						}
						else
						{
							sb.Append("'").Append(SanitizeSqlValue(identifiers[j])).Append("'");
						}
						
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

		public string SanitizeSqlValue(string sqlValue)
		{
			return sqlValue.Replace("'", "''");
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


		private static void SetParamValue(DbParameter colParam, RavenJToken val, List<Func<DbParameter, String, Boolean>> stringParsers)
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
						if( value.Length > 0 && stringParsers != null ) {
							foreach( var parser in stringParsers ) {
								if( parser( colParam, value ) ) {
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

		private DbProviderFactory GetDbProviderFactory(SqlReplicationConfig cfg)
		{
			DbProviderFactory providerFactory;
			try
			{
				providerFactory = DbProviderFactories.GetFactory(cfg.FactoryName);
			}
			catch (Exception e)
			{
				log.WarnException(
					string.Format("Could not find provider factory {0} to replicate to sql for {1}, ignoring", cfg.FactoryName,
									cfg.Name), e);

				database.AddAlert(new Alert
				{
					AlertLevel = AlertLevel.Error,
					CreatedAt = SystemTime.UtcNow,
					Exception = e.ToString(),
					Title = "Sql Replication could not find factory provider",
					Message = string.Format("Could not find factory provider {0} to replicate to sql for {1}, ignoring", cfg.FactoryName,
									cfg.Name),
					UniqueKey = string.Format("Sql Replication Provider Not Found: {0}, {1}", cfg.Name, cfg.FactoryName)
				});

				throw;
			}
			return providerFactory;
		}



		public void Dispose()
		{
			tx.Dispose();
			commandBuilder.Dispose();
			connection.Dispose();
		}
	}
}