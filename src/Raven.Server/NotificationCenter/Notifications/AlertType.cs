// ReSharper disable InconsistentNaming
namespace Raven.Server.NotificationCenter.Notifications
{
    public enum AlertType
    {
        Etl_Error,
        Etl_TransformationError,
        Etl_LoadError,

        SqlEtl_ConnectionError,
        SqlEtl_ProviderError,
        SqlEtl_SlowSql,
        SqlEtl_ConnectionStringMissing,
        
        Etl_WriteErrorRatio,
        
        PeriodicExport,
        Replication,
        Server_NewVersionAvailable,
        LicenseManager_InitializationError,
        IndexStore_IndexCouldNotBeOpened,
        TransformerStore_TransformerCouldNotBeOpened,
        WarnIndexOutputsPerDocument,
        ErrorSavingReduceOutputDocuments,
        CatastrophicDatabaseFailue
    }
}