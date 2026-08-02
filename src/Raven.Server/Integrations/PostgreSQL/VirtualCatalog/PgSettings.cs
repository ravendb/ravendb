using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal static class PgSettings
    {
        private static readonly SortedDictionary<string, string> Values = new(StringComparer.OrdinalIgnoreCase)
        {
            ["max_index_keys"] = "32",
            ["lc_collate"] = "C",
            ["lc_ctype"] = "C",
            ["lc_monetary"] = "C",
            ["lc_numeric"] = "C",
            ["lc_time"] = "C",
            ["server_encoding"] = "UTF8",
            ["client_encoding"] = "UTF8",
            ["default_tablespace"] = "",
            ["search_path"] = "\"$user\", public",
            ["timezone"] = "UTC",

            ["server_version"] = "13.3",
            ["server_version_num"] = "130003",
            ["standard_conforming_strings"] = "on",
            ["integer_datetimes"] = "on",

            // RavenDB has no PG-style isolation; PG's default keeps drivers off serializable-retry paths.
            ["transaction_isolation"] = "read committed",
        };

        public static bool TryGetValue(string name, out string value)
        {
            value = null;
            if (string.IsNullOrEmpty(name))
                return false;
            return Values.TryGetValue(name, out value);
        }

        public static IEnumerable<KeyValuePair<string, string>> All => Values.ToList();
    }
}
