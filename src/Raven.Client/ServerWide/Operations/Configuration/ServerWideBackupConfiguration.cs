using Raven.Client.Documents.Operations.Backups;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class ServerWideBackupConfiguration : PeriodicBackupConfiguration
    {
        internal static string NamePrefix = "Server Wide Backup Configuration";
    }
}
