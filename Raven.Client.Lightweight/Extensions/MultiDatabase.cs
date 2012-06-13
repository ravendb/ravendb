using System;
using System.IO;
using System.Linq;
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
			                                          				{"Raven/DataDir", Path.Combine("~", Path.Combine("Tenants", name))}
			                                          			}
			                                          	});
			doc.Remove("Id");
			return doc;
		}

		private static readonly string[] invalidDbNameChars = new[] {"/", "\\", "\"", "'", "<", ">"};

		private static void AssertValidName(string name)
		{
			if (name == null) throw new ArgumentNullException("name");
			if (invalidDbNameChars.Any(name.Contains))
			{
				throw new ArgumentException("Database name cannot contain any of [" +
				                            string.Join(", ", invalidDbNameChars) + "] but was: " + name);
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