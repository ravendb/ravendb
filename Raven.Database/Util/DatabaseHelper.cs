// -----------------------------------------------------------------------
//  <copyright file="DatabaseHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Extensions;

namespace Raven.Database.Util
{
	public static class DatabaseHelper
	{
		public static bool IsSystemDatabase(this DocumentDatabase database)
		{
			return database.Name == null || database.Name == Constants.SystemDatabase;
		}

		public static void AssertSystemDatabase(DocumentDatabase database)
		{
			if (database.IsSystemDatabase() == false)
				throw new InvalidOperationException("Not a system database");
		}

		public static string GetDatabaseKey(string key)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (key.StartsWith(Constants.Database.Prefix, StringComparison.OrdinalIgnoreCase))
				return key;

			return Constants.Database.Prefix + key;
		}

		public static string GetDatabaseName(string key)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (key.StartsWith(Constants.Database.Prefix, StringComparison.OrdinalIgnoreCase))
				return key.Substring(Constants.Database.Prefix.Length);

			return key;
		}

		public static void DeleteDatabaseFiles(InMemoryRavenConfiguration configuration)
		{
			if (configuration.RunInMemory)
				return;

			IOExtensions.DeleteDirectory(configuration.DataDirectory);
			if (configuration.IndexStoragePath != null)
				IOExtensions.DeleteDirectory(configuration.IndexStoragePath);

			if (configuration.Storage.Esent.JournalsStoragePath != null)
				IOExtensions.DeleteDirectory(configuration.Storage.Esent.JournalsStoragePath);

			if (configuration.Storage.Voron.JournalsStoragePath != null)
				IOExtensions.DeleteDirectory(configuration.Storage.Voron.JournalsStoragePath);
		}
	}
}