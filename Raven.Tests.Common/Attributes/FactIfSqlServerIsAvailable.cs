using System;
using System.Configuration;
using System.Data.Common;

using Xunit;

namespace Raven.Tests.Common.Attributes
{
    public class MaybeSqlServerIsAvailable
    {
        private static bool triedLoading;
        private static ConnectionStringSettings sqlServerConnectionStringSettings;
        public static ConnectionStringSettings SqlServerConnectionStringSettings
        {
            get
            {
                if (sqlServerConnectionStringSettings == null)
                {
                    var skipException = new SkipException("Cannot execute this test, because there are no sql server valid connection strings in this machine");

                    if (triedLoading)
                        throw skipException;

                    lock (typeof(MaybeSqlServerIsAvailable))
                    {
                        triedLoading = true;
                        sqlServerConnectionStringSettings = GetAppropriateConnectionStringNameInternal(GetPossibleSqlServerConnectionStrings());
                        if (sqlServerConnectionStringSettings == null)
                            throw skipException;
                    }
                }
                return sqlServerConnectionStringSettings;
            }
        }
        private static ConnectionStringSettings postgresConnectionStringSettings;
        public static ConnectionStringSettings PostgresConnectionStringSettings
        {
            get
            {
                if (postgresConnectionStringSettings == null)
                {
                    var skipException = new SkipException("Cannot execute this test, because there are no postgres valid connection strings in this machine");

                    if (triedLoading)
                        throw skipException;

                    lock (typeof(MaybeSqlServerIsAvailable))
                    {
                        triedLoading = true;
                        postgresConnectionStringSettings = GetAppropriateConnectionStringNameInternal(GetPossiblePostgresConnectionStrings());
                        if (postgresConnectionStringSettings == null)
                            throw skipException;
                    }
                }
                return postgresConnectionStringSettings;
            }
        }

    
        private static ConnectionStringSettings GetAppropriateConnectionStringNameInternal(ConnectionStringSettings[] possibleConnectionStrings)
        {
            foreach (var settings in possibleConnectionStrings)
            {
                if (settings == null)
                    continue;

                var connectionStringName = settings.Name;
                var connectionStringProvider = settings.ProviderName;
                var connectionString = settings.ConnectionString;

                if (string.Equals(connectionStringName, "CiHost", StringComparison.OrdinalIgnoreCase))
                    connectionString = connectionString.Replace("Initial Catalog=Raven.Tests", "Initial Catalog=Raven.Tests" + Environment.MachineName);

                var localSettings = new ConnectionStringSettings(connectionStringName, connectionString, connectionStringProvider);

                var providerFactory = DbProviderFactories.GetFactory(localSettings.ProviderName);
                try
                {
                    using (var connection = providerFactory.CreateConnection())
                    {
                        connection.ConnectionString = localSettings.ConnectionString;
                        connection.Open();
                    }
                    return localSettings;
                }
                    // ReSharper disable EmptyGeneralCatchClause
                catch
                    // ReSharper restore EmptyGeneralCatchClause
                {
                }
            }
            return null;
        }

        private static ConnectionStringSettings[] GetPossibleSqlServerConnectionStrings()
        {
            return new[]
            {
                ConfigurationManager.ConnectionStrings["SqlExpress"],
                ConfigurationManager.ConnectionStrings["LocalHost"],
                ConfigurationManager.ConnectionStrings["CiHost"]
            };
        }
        private static ConnectionStringSettings[] GetPossiblePostgresConnectionStrings()
        {
            return new[]
            {
                ConfigurationManager.ConnectionStrings["Postgres"],
                ConfigurationManager.ConnectionStrings["CiHostPostgres"]
            };
        }
    }
}
