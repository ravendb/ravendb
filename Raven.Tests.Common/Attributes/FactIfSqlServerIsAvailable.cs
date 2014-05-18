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
			foreach (ConnectionStringSettings connectionString in new[]
			{
				ConfigurationManager.ConnectionStrings["SqlExpress"],
				ConfigurationManager.ConnectionStrings["LocalHost"],
			})
			{
				if(connectionString == null)
					continue;

				var providerFactory = DbProviderFactories.GetFactory(connectionString.ProviderName);
				try
				{
					using (var connection = providerFactory.CreateConnection())
					{
						connection.ConnectionString = connectionString.ConnectionString;
						connection.Open();
					}
					return connectionString;
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