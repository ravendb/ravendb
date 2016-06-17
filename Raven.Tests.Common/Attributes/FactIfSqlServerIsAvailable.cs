using System;
using System.Configuration;
using System.Data.Common;

using Xunit;

namespace Raven.Tests.Common.Attributes
{
    public class MaybeSqlServerIsAvailable
    {
        private static bool triedLoading;
        private static ConnectionStringSettings connectionStringSettings;
        public static ConnectionStringSettings ConnectionStringSettings
        {
            get
            {
                if (connectionStringSettings == null)
                {
                    var skipException = new SkipException("Cannot execute this test, because there are no valid connection strings in this machine");

                    if (triedLoading)
                        throw skipException;

                    lock (typeof(MaybeSqlServerIsAvailable))
                    {
                        triedLoading = true;
                        connectionStringSettings = GetAppropriateConnectionStringNameInternal();
                        if (connectionStringSettings == null)
                            throw skipException;
                    }
                }
                return connectionStringSettings;
            }
        }

    
        private static ConnectionStringSettings GetAppropriateConnectionStringNameInternal()
        {
            foreach (var settings in new[]
            {
                ConfigurationManager.ConnectionStrings["PostgreSQL"],
                ConfigurationManager.ConnectionStrings["SqlExpress"],
                ConfigurationManager.ConnectionStrings["LocalHost"],
                ConfigurationManager.ConnectionStrings["CiHost"]
            })
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
    }
}
