using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class ServerWideBackupConfiguration : PeriodicBackupConfiguration
    {
        public static string ConfigurationName = "Server Wide Backup Configuration";

        [JsonDeserializationIgnore]
        public override string Name
        {
            get => ConfigurationName;
            set => throw new NotSupportedException("Setting the Name in ServerWideBackupConfiguration isn't supported");
        }
    }
}
