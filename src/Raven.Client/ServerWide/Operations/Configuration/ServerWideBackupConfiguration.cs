using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class ServerWideBackupConfiguration : PeriodicBackupConfiguration
    {
        internal static string NamePrefix = "Server Wide Backup";
        
        // Properties to be used by the studio server-wide backups list view
        public OngoingTaskState TaskState { get; set; }
        public List<string> BackupDestinations { get; set; }
        
        public ServerWideBackupConfiguration()
        {
            BackupDestinations = new List<string>();
        }
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(NamePrefix)] = NamePrefix;
            json[nameof(TaskState)] = TaskState;
            json[nameof(BackupDestinations)] = new DynamicJsonArray(BackupDestinations);
            
            return json;
        }
    }
}
