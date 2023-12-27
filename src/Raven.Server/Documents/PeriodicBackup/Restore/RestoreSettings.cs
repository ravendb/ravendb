using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreSettings
    {
        public RestoreSettings()
        {
            DatabaseValues = new Dictionary<string, BlittableJsonReaderObject>();
            Subscriptions = new Dictionary<string, SubscriptionState>();
        }

        public static string SettingsFileName = "Settings.json";

        public static string SmugglerValuesFileName = "SmugglerValues.ravendump";

        public DatabaseRecord DatabaseRecord { get; set; }

        public Dictionary<string, BlittableJsonReaderObject> DatabaseValues { get; set; }

        public Dictionary<string, SubscriptionState> Subscriptions;
    }
}
