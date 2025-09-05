namespace Voron.Impl.Backup
{
    public static class BackupMethods
    {
        public const string Filename = "RavenDB.Voron.Backup";

        public static FullBackup Full = new FullBackup();

        public static IncrementalBackup Incremental = new IncrementalBackup();
    }
}