using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Client.Extensions
{
	///<summary>
	/// Methods to create mutli tenants databases
	///</summary>
	internal static class MultiDatabase
	{
		public static RavenJObject CreateDatabaseDocument(string name)
		{
			AssertValidName(name);
			var doc = RavenJObject.FromObject(new DatabaseDocument
			                                          	{
			                                          		Settings =
			                                          			{
			                                          				{"Raven/DataDir", Path.Combine("~", Path.Combine("Databases", name))}
			                                          			}
			                                          	});
			doc.Remove("Id");
			return doc;
		}

		private static readonly string validDbNameChars = @"([A-Za-z0-9_\-\.]+)";

		private static void AssertValidName(string name)
		{
			if (name == null) throw new ArgumentNullException("name");
			var result = Regex.Matches(name, validDbNameChars);
			if (result.Count == 0 || result[0].Value != name)
			{
				throw new InvalidOperationException("Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + name);
			}
		}

		public static string GetRootDatabaseUrl(string url)
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