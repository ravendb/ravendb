using System;
using System.Configuration;
using System.Data.Common;

using Xunit;

namespace Raven.Tests.Common.Attributes
{
    public class MaybeSqlServerIsAvailable
    {
        private static bool triedLoading = false;
        private static ConnectionStringSettings connectionStringSettings;
        public static ConnectionStringSettings ConnectionStringSettings
        {
            get
            {
                if (connectionStringSettings == null)
                {
                    var skipException = new SkipException("Cannot execute this test, because there are no valid connection strings in this machine");

                    if(triedLoading)
                        throw skipException;

                    lock (typeof(MaybeSqlServerIsAvailable))
                    {
                        triedLoading = true;
                        connectionStringSettings = GetAppropriateConnectionStringNameInternal();
                        if(connectionStringSettings == null)
                            throw skipException;
                    }
                }
                return connectionStringSettings;
            }
        }

    
        private static ConnectionStringSettings GetAppropriateConnectionStringNameInternal()
        {
            foreach (var connectionString in new[]
            {
                ConfigurationManager.ConnectionStrings["SqlExpress"],
                ConfigurationManager.ConnectionStrings["LocalHost"],
                ConfigurationManager.ConnectionStrings["CiHost"],
            })
            {
                if(connectionString == null)
                    continue;

                var conn = connectionString;
                if (connectionString.Name == "CiHost")
                {
                    conn = new ConnectionStringSettings(connectionString.Name, connectionString.ConnectionString.Replace("Initial Catalog=Raven.Tests", "Initial Catalog=Raven.Tests" + Environment.MachineName), connectionString.ProviderName);
                }

                var providerFactory = DbProviderFactories.GetFactory(conn.ProviderName);
                try
                {
                    using (var connection = providerFactory.CreateConnection())
                    {
                        connection.ConnectionString = conn.ConnectionString;
                        connection.Open();
                    }
                    return conn;
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
