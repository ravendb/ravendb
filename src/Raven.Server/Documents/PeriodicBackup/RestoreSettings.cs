using System.Collections.Generic;
using Raven.Client.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RestoreSettings
    {
        public static string FileName = "Settings.json";

        public DatabaseRecord DatabaseRecord { get; set; }

        public Dictionary<string, long> Identities { get; set; }

        public Dictionary<string, object> DatabaseValues { get; set; }
    }
}