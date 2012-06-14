using System;
using System.Configuration;
using System.Data.Common;
using Xunit;

namespace Raven.Bundles.Tests.IndexReplication
{
	[CLSCompliant(false)]
	public class FactIfSqlServerIsAvailable : FactAttribute
	{
		public static ConnectionStringSettings ConnectionStringSettings { get; set; }

		public FactIfSqlServerIsAvailable()
		{
			ConnectionStringSettings = GetAppropriateConnectionStringNameInternal();
			if (ConnectionStringSettings == null)
			{
				base.Skip = "Could not find a connection string with a valid database to connect to, skipping the test";
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