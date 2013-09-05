// -----------------------------------------------------------------------
//  <copyright file="FailoverServers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class FailoverServers
	{
		private readonly HashSet<string> forDefaultDatabase = new HashSet<string>();
		private readonly IDictionary<string, HashSet<string>> forDatabases = new Dictionary<string, HashSet<string>>();

		public string[] ForDefaultDatabase
		{
			get
			{
				var result = new string[forDefaultDatabase.Count];
				forDefaultDatabase.CopyTo(result);
				return result;
			}
			set { AddForDefaultDatabase(value); }
		}

		public IDictionary<string, string[]> ForDatabases
		{
			set
			{
				foreach (var specificDatabaseServers in value)
				{
					AddForDatabase(specificDatabaseServers.Key, specificDatabaseServers.Value);
				}
			}
		}

		public bool IsSetForDefaultDatabase
		{
			get { return forDefaultDatabase.Count > 0; }
		}

		public bool IsSetForDatabase(string databaseName)
		{
			return forDatabases.Keys.Contains(databaseName) && forDatabases[databaseName] != null && forDatabases[databaseName].Count > 0;
		}

		public string[] GetForDatabase(string databaseName)
		{
			if (forDatabases.Keys.Contains(databaseName) == false || forDatabases[databaseName] == null)
				return null;

			var result = new string[forDatabases[databaseName].Count];
			forDatabases[databaseName].CopyTo(result);
			return result;
		}

		public void AddForDefaultDatabase(params string[] urls)
		{
			foreach (var url in urls)
			{
				forDefaultDatabase.Add(url.EndsWith("/") ? url.Substring(0, url.Length - 1) : url);
			}
		}

		public void AddForDatabase(string databaseName, params string[] urls)
		{
			if (forDatabases.Keys.Contains(databaseName) == false)
			{
				forDatabases[databaseName] = new HashSet<string>();
			}

			foreach (var url in urls)
			{
				forDatabases[databaseName].Add(url.EndsWith("/") ? url.Substring(0, url.Length - 1) : url);
			}
		}
	}
}