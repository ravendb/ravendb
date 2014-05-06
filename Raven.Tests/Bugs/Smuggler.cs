using System;
using Raven.Abstractions.Data;
using Raven.Smuggler;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class Smuggler : NoDisposalNeeded
	{
		[Fact]
		public void should_respect_defaultdb_properly()
		{
			var connectionStringOptions = new RavenConnectionStringOptions {Url = "http://localhost:8080", DefaultDatabase = "test"};
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