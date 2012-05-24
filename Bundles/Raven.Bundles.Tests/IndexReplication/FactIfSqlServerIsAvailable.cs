using System;
using System.Configuration;
using System.Data.Common;
using System.Linq;
using System.Xml;
using Xunit;
using Xunit.Sdk;

namespace Raven.Bundles.Tests.IndexReplication
{
	[CLSCompliant(false)]
	public class FactIfSqlServerIsAvailable : FactAttribute
	{
		readonly ConnectionStringSettings connectionStringSettings;

		public FactIfSqlServerIsAvailable()
		{
			var connectionStringName = GetAppropriateConnectionStringNameInternal();
			if(connectionStringName == null)
			{
				base.Skip = "Could not find a connection string with a valid database to connect to, skipping the test";
				return;
			}
			connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
		}

		protected override System.Collections.Generic.IEnumerable<Xunit.Sdk.ITestCommand> EnumerateTestCommands(Xunit.Sdk.IMethodInfo method)
		{
			return base.EnumerateTestCommands(method).Select(enumerateTestCommand => new ActionTestCommandWrapper(enumerateTestCommand, o =>
			{
				((ReplicateToSql)o).ConnectionString=connectionStringSettings;
			}));
		}

		public class ActionTestCommandWrapper : ITestCommand
		{
			private readonly ITestCommand inner;
			private readonly Action<object> action;

			public ActionTestCommandWrapper(ITestCommand inner, Action<object> action)
			{
				this.inner = inner;
				this.action = action;
			}

			public MethodResult Execute(object testClass)
			{
				action(testClass);
				return inner.Execute(testClass);
			}

			public XmlNode ToStartXml()
			{
				return inner.ToStartXml();
			}

			public string DisplayName
			{
				get { return inner.DisplayName; }
			}

			public bool ShouldCreateInstance
			{
				get { return inner.ShouldCreateInstance; }
			}

			public int Timeout
			{
				get { return inner.Timeout; }
			}
		}

		private static string GetAppropriateConnectionStringNameInternal()
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
					return connectionString.Name;
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