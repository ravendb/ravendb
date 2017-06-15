using System.Collections.Generic;
using Raven.Client.Server;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RestoreSettings
    {
        public static string FileName = "Settings.json";

        public DatabaseRecord DatabaseRecord { get; set; }

        public Dictionary<string, object> DatabaseValues { get; set; }
    }
}