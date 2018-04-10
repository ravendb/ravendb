using System.Collections.Generic;
using System.Dynamic;
using Raven.Client.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreSettings
    {
        public RestoreSettings()
        {
            DatabaseValues = new Dictionary<string, ExpandoObject>();
        }

        public static string SettingsFileName = "Settings.json";

        public static string SmugglerValuesFileName = "SmugglerValues.ravendump";

        public DatabaseRecord DatabaseRecord { get; set; }

        public Dictionary<string, ExpandoObject> DatabaseValues { get; set; }
    }
}
