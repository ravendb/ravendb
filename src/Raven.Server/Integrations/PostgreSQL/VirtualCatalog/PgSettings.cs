using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    /// <summary>
    /// The single source of truth for the run-time configuration parameters (PG calls them GUCs)
    /// this bridge reports. Real PG reads these from postgresql.conf / the active session; we hand
    /// back static values that describe how RavenDB's PG endpoint actually behaves (UTF-8
    /// everywhere, one namespace, read-only).
    /// <para>
    /// Both readers of a setting go through here:
    /// <list type="bullet">
    ///   <item><c>current_setting('x')</c> - see <see cref="CurrentSettingFunction"/></item>
    ///   <item><c>SHOW x</c> - see <see cref="PgShowStatement"/></item>
    /// </list>
    /// They must never disagree: clients probe whichever form their driver prefers and some (e.g.
    /// SQLAlchemy) read both across a single session, so two tables would surface as a client
    /// seeing the setting flip value mid-connection.
    /// </para>
    /// Lookup is case-insensitive, matching PG - GUC names are not case-sensitive.
    /// </summary>
    internal static class PgSettings
    {
        private static readonly SortedDictionary<string, string> Values = new(StringComparer.OrdinalIgnoreCase)
        {
            // The settings pgAdmin probes for during connection / property inspection.
            ["max_index_keys"] = "32",
            ["lc_collate"] = "C",
            ["lc_ctype"] = "C",
            ["lc_monetary"] = "C",
            ["lc_numeric"] = "C",
            ["lc_time"] = "C",
            ["server_encoding"] = "UTF8",
            ["client_encoding"] = "UTF8",
            ["default_tablespace"] = "",            // empty string ⇒ pg_default
            ["search_path"] = "\"$user\", public",
            ["timezone"] = "UTC",

            // Version probes. Many drivers / BI tools call current_setting('server_version')
            // (and the *_num form) right after connecting to decide which SQL dialect features
            // to use. Mirror the 13.3 banner reported by version() (see VersionFunction).
            ["server_version"] = "13.3",
            ["server_version_num"] = "130003",
            ["standard_conforming_strings"] = "on",
            ["integer_datetimes"] = "on",

            // SQLAlchemy's PGDialect.get_isolation_level() issues `show transaction isolation
            // level` on every connect and READS the value back, so this cannot be a no-op.
            //
            // "read committed" is a SEMANTIC CHOICE, not a measured fact about this bridge:
            // RavenDB has no PG-style transaction isolation, and PgTransaction on this surface
            // does not span statements. We report PG's own default so drivers take their
            // ordinary code path instead of the serializable-retry logic some enable when they
            // see a stricter level. If RavenDB's real guarantee over this bridge turns out to be
            // weaker or stronger, this is the line to change.
            ["transaction_isolation"] = "read committed",
        };

        public static bool TryGetValue(string name, out string value)
        {
            value = null;
            if (string.IsNullOrEmpty(name))
                return false;
            return Values.TryGetValue(name, out value);
        }

        // Every setting, ordered by name - `SHOW ALL` reports them in name order like PG does.
        public static IEnumerable<KeyValuePair<string, string>> All => Values.ToList();
    }
}
