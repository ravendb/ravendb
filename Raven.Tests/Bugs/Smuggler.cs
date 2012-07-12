using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SmugglerTester : SmugglerApi
	{
		public SmugglerTester(RavenConnectionStringOptions connectionStringOptions)
			: base(new SmugglerOptions(), connectionStringOptions)
		{
		}

		public string GetUrlGeneratedForRequest(string url, string method = "GET")
		{
			return CreateRequest(url, method).WebRequest.RequestUri.AbsoluteUri;
		}
	}

	public class Smuggler
	{
		[Fact]
		public void should_respect_defaultdb_properly()
		{
			var connectionStringOptions = new RavenConnectionStringOptions();
			//SmugglerAction action = SmugglerAction.Import;
			connectionStringOptions.Url = "http://localhost:8080";
			connectionStringOptions.DefaultDatabase = "test";

			var api = new SmugglerTester(connectionStringOptions);
			var rootDatabaseUrl = GetRootDatabaseUrl(connectionStringOptions.Url);
			var docUrl = rootDatabaseUrl + "/docs/Raven/Databases/" + connectionStringOptions.DefaultDatabase;
			Console.WriteLine(docUrl);
		}

		private static string GetRootDatabaseUrl(string url)
		{
			var databaseUrl = url;
			var indexOfDatabases = databaseUrl.IndexOf("/databases/", StringComparison.Ordinal);
			if (indexOfDatabases != -1)
				databaseUrl = databaseUrl.Substring(0, indexOfDatabases);
			if (databaseUrl.EndsWith("/"))
				return databaseUrl.Substring(0, databaseUrl.Length - 1);
			return databaseUrl;
		}
	}
}