namespace Raven.Server.Documents.PeriodicBackup
{
    public enum PeriodicBackupTestConnectionType
    {
        None,
        
        Local,
        S3,
        Glacier,
        Azure
    }
}