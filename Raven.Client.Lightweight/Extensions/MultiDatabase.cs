using System;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;

namespace Raven.Client.Extensions
{
	///<summary>
	/// Methods to create multitenant databases
	///</summary>
	internal static class MultiDatabase
	{
		public static DatabaseDocument CreateDatabaseDocument(string name)
		{
			return new DatabaseDocument
			{
				Id = "Raven/Databases/" + name,
				Settings =
				{
					{"Raven/DataDir", Path.Combine("~", Path.Combine("Databases", name))}
				}
			};
		}

		private const string ValidDbNameChars = @"([A-Za-z0-9_\-\.]+)";

		public static void AssertValidDatabaseName(string name)
		{
			if (name == null) throw new ArgumentNullException("name");
			var result = Regex.Matches(name, ValidDbNameChars);
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

		public static string GetDatabaseName(string url)
		{
			if (url == null)
				return null;

			var databaseUrl = url;
			var indexOfDatabases = databaseUrl.IndexOf("/databases/", StringComparison.Ordinal);
			if (indexOfDatabases != -1)
			{
				databaseUrl = databaseUrl.Substring(indexOfDatabases + "/databases/".Length);
				return Regex.Match(databaseUrl, ValidDbNameChars).Value;
			}

			return Constants.SystemDatabase;
		}
	}
}