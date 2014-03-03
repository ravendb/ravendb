using System;
using System.Configuration;
using System.Data.Common;
using Xunit;

namespace Raven.Tests.Bundles.SqlReplication
{
	public class FactIfSqlServerIsAvailable
	{
		public static ConnectionStringSettings ConnectionStringSettings { get; set; }

		static FactIfSqlServerIsAvailable()
		{
			ConnectionStringSettings = GetAppropriateConnectionStringNameInternal();
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