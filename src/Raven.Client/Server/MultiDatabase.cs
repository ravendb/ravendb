using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Server
{
    ///<summary>
    /// Methods to create multi-tenant databases
    ///</summary>
    public static class MultiDatabase
    {
        public static DatabaseRecord CreateDatabaseDocument(string name)
        {
            AssertValidName(name);

            return new DatabaseRecord(name)
            {
                Settings = new Dictionary<string, string>()
            };
        }

        private const string ValidDbNameChars = @"([A-Za-z0-9_\-\.]+)";

        internal static void AssertValidName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (name.Length > Constants.Documents.MaxDatabaseNameLength)
                throw new InvalidOperationException($"Name '{name}' exceeds {Constants.Documents.MaxDatabaseNameLength} characters.");

            if (name.Equals(Constants.Documents.SystemDatabase, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("<system> is not valid name. We don't have system database anymore.");

            var result = Regex.Matches(name, ValidDbNameChars);
            if (result.Count == 0 || result[0].Value != name)
            {
                throw new InvalidOperationException(
                    "Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + name);
            }
        }

        /// <summary>
        ///  Returns database url (system or non-system) based on system or non-system DB url.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="database">The database name.</param>
        /// <returns></returns>
        public static string GetDatabaseUrl(string url, string database)
        {
            if (database == Constants.Documents.SystemDatabase)
            {
                return GetRootDatabaseUrl(url);
            }
            return GetRootDatabaseUrl(url) + "/databases/" + database + "/";
        }

        public static string GetRootDatabaseUrl(string url)
        {
            var databaseUrl = url;
            var indexOfDatabases = databaseUrl.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
            if (indexOfDatabases != -1)
                databaseUrl = databaseUrl.Substring(0, indexOfDatabases);
            if (databaseUrl.EndsWith("/"))
                return databaseUrl.Substring(0, databaseUrl.Length - 1);
            return databaseUrl;
        }

        public static string GetRootFileSystemUrl(string url)
        {
            var fileSystemUrl = url;
            var indexOfDatabases = fileSystemUrl.IndexOf("/fs/", StringComparison.OrdinalIgnoreCase);
            if (indexOfDatabases != -1)
                fileSystemUrl = fileSystemUrl.Substring(0, indexOfDatabases);
            if (fileSystemUrl.EndsWith("/"))
                return fileSystemUrl.Substring(0, fileSystemUrl.Length - 1);
            return fileSystemUrl;
        }

        public static string GetDatabaseName(string url)
        {
            if (url == null)
                return null;

            var databaseUrl = url;
            var indexOfDatabases = databaseUrl.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
            if (indexOfDatabases != -1)
            {
                databaseUrl = databaseUrl.Substring(indexOfDatabases + "/databases/".Length);
                return Regex.Match(databaseUrl, ValidDbNameChars).Value;
            }

            return Constants.Documents.SystemDatabase;
        }
    }
}
