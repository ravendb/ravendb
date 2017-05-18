using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Server
{

    //TODO: kill this class
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

            var result = Regex.Matches(name, ValidDbNameChars);
            if (result.Count == 0 || result[0].Value != name)
            {
                throw new InvalidOperationException(
                    "Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + name);
            }
        }
    }
}
