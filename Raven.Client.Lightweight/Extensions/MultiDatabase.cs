using System;
using System.IO;
using System.Text.RegularExpressions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.TimeSeries;

namespace Raven.Client.Extensions
{
    ///<summary>
    /// Methods to create multitenant databases
    ///</summary>
    public static class MultiDatabase
    {
        public static Action<DatabaseDocument> ConfigureDatabaseDocument { get; set; }

        public static DatabaseDocument CreateDatabaseDocument(string name)
        {
            AssertValidName(name);
            if (name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
                return new DatabaseDocument { Id = Constants.SystemDatabase };

            var dbDoc = new DatabaseDocument
            {
                Id = "Raven/Databases/" + name,
                Settings =
                {
                    {"Raven/DataDir", Path.Combine("~", name)},
                }
            };
            ConfigureDatabaseDocument?.Invoke(dbDoc);
            return dbDoc;
        }
        public static FileSystemDocument CreateFileSystemDocument(string name)
        {
            AssertValidName(name);

            return new FileSystemDocument
            {
                Id = Constants.FileSystem.Prefix + name,
                Settings =
                {
                    {Constants.FileSystem.DataDirectory, Path.Combine("~", "FileSystems", name) },
                }
            };
        }

        public static TimeSeriesDocument CreateTimeSeriesDocument(string name)
        {
            AssertValidName(name);

            return new TimeSeriesDocument
            {
                Id = Constants.TimeSeries.Prefix + name,
                Settings =
                {
                    {Constants.TimeSeries.DataDirectory, Path.Combine("~", "TimeSeries", name)},
                }
            };
        }

        public static CounterStorageDocument CreateCounterStorageDocument(string name)
        {
            AssertValidName(name);

            return new CounterStorageDocument
            {
                Id = Constants.Counter.Prefix + name,
                Settings =
                {
                    {Constants.Counter.DataDirectory, Path.Combine("~", "Counters", name)},
                }
            };
        }

        private const string ValidDbNameChars = @"([A-Za-z0-9_\-\.]+)";

        internal static void AssertValidName(string name)
        {
            if (name == null) throw new ArgumentNullException("name");

            if (name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
                return;

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
            if (database == Constants.SystemDatabase)
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

            return Constants.SystemDatabase;
        }
    }
}
