using System;
using System.Collections.Generic;

namespace Raven.Client.Util
{
    internal class DatabaseChangesOptions
    {
        public string DatabaseName { get; set; }
        public string NodeTag { get; set; }
    }

    internal class DatabaseChangesOptionsComparer : IEqualityComparer<DatabaseChangesOptions>
    {
        private readonly IEqualityComparer<string> _stringComparer;
        public static DatabaseChangesOptionsComparer OrdinalIgnoreCase = new DatabaseChangesOptionsComparer(StringComparer.OrdinalIgnoreCase);

        public DatabaseChangesOptionsComparer(IEqualityComparer<string> strComparer)
        {
            _stringComparer = strComparer;
        }

        public bool Equals(DatabaseChangesOptions n1, DatabaseChangesOptions n2)
        {
            return _stringComparer.Equals(n1.DatabaseName, n2.DatabaseName) && _stringComparer.Equals(n1.NodeTag, n2.NodeTag);
        }

        public int GetHashCode(DatabaseChangesOptions n)
        {
            return n.GetHashCode();
        }
    }
}
