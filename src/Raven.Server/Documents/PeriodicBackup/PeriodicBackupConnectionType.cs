namespace Raven.Server.Documents.PeriodicBackup
{
    public enum PeriodicBackupConnectionType
    {
        None,
        
        Local,
        S3,
        Glacier,
        Azure,
        GoogleCloud,
        FTP
    }
}
