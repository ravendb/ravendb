using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class ServerWideBackupConfiguration : PeriodicBackupConfiguration
    {
        internal static string NamePrefix = "Server Wide Backup";

        public string[] DatabasesToExclude { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(NamePrefix)] = NamePrefix;
            json[nameof(DatabasesToExclude)] = DatabasesToExclude;
            return json;
        }
    }
}
