using System.Collections.Generic;
using System.Dynamic;
using Raven.Client.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RestoreSettings
    {
        public RestoreSettings()
        {
            DatabaseValues = new Dictionary<string, ExpandoObject>();
            Identities = new Dictionary<string, long>();
        }

        public static string FileName = "Settings.json";

        public DatabaseRecord DatabaseRecord { get; set; }

        public Dictionary<string, ExpandoObject> DatabaseValues { get; set; }

        public Dictionary<string, long> Identities { get; set; }
    }
}
