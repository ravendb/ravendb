using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Documents.PeriodicBackup;

public sealed class UploaderSettings
{
    public readonly Config.Categories.BackupConfiguration Configuration;

    public UploaderSettings(Config.Categories.BackupConfiguration configuration)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    public S3Settings S3Settings;
    public GlacierSettings GlacierSettings;
    public AzureSettings AzureSettings;
    public GoogleCloudSettings GoogleCloudSettings;
    public FtpSettings FtpSettings;

    public string FilePath;
    public string FolderName;
    public string FileName;
    public string DatabaseName;
    public string TaskName;

    public BackupType? BackupType;
    public Action OnBackupException;
    internal BackupConfiguration.BackupDestination Destination;
    public short ConcurrentThreads { get; set; }

    public static UploaderSettings GenerateUploaderSetting(DocumentDatabase database, string taskName, S3Settings s3Settings, AzureSettings azureSettings, GlacierSettings glacierSettings, GoogleCloudSettings googleCloudSettings, FtpSettings ftpSettings)
    {
        return new UploaderSettings(database.Configuration.Backup)
        {
            S3Settings = BackupTask.GetBackupConfigurationFromScript(s3Settings, x => JsonDeserializationServer.S3Settings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            AzureSettings = BackupTask.GetBackupConfigurationFromScript(azureSettings, x => JsonDeserializationServer.AzureSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            GlacierSettings = BackupTask.GetBackupConfigurationFromScript(glacierSettings, x => JsonDeserializationServer.GlacierSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            GoogleCloudSettings = BackupTask.GetBackupConfigurationFromScript(googleCloudSettings, x => JsonDeserializationServer.GoogleCloudSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            FtpSettings = BackupTask.GetBackupConfigurationFromScript(ftpSettings, x => JsonDeserializationServer.FtpSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            DatabaseName = database.Name,
            TaskName = taskName
        };
    }

    public static UploaderSettings GenerateDirectUploaderSetting(DocumentDatabase database, string taskName, S3Settings s3Settings, AzureSettings azureSettings, GlacierSettings glacierSettings, GoogleCloudSettings googleCloudSettings, FtpSettings ftpSettings, short concurrentThreads)
    {
        var destination = BackupConfigurationHelper.DestinationForDirectUpload(database.Configuration.Backup, s3Settings, azureSettings, glacierSettings, googleCloudSettings, ftpSettings);
        return new UploaderSettings(database.Configuration.Backup)
        {
            S3Settings = BackupTask.GetBackupConfigurationFromScript(s3Settings, x => JsonDeserializationServer.S3Settings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            AzureSettings = BackupTask.GetBackupConfigurationFromScript(azureSettings, x => JsonDeserializationServer.AzureSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            GlacierSettings = BackupTask.GetBackupConfigurationFromScript(glacierSettings, x => JsonDeserializationServer.GlacierSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            GoogleCloudSettings = BackupTask.GetBackupConfigurationFromScript(googleCloudSettings, x => JsonDeserializationServer.GoogleCloudSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            FtpSettings = BackupTask.GetBackupConfigurationFromScript(ftpSettings, x => JsonDeserializationServer.FtpSettings(x),
                database, updateServerWideSettingsFunc: null, serverWide: false),
            DatabaseName = database.Name,
            TaskName = taskName,
            Destination = destination,
            ConcurrentThreads = concurrentThreads
        };
    }
    public static UploaderSettings GenerateUploaderSettingForBackup(DocumentDatabase database, BackupConfiguration configuration, string taskName, bool isServerWide, bool backupToLocalFolder,
        Action backupException)
    {
        var destination = BackupConfigurationHelper.GetBackupDestinationForDirectUpload(backupToLocalFolder, configuration, database.Configuration.Backup);
        return new UploaderSettings(database.Configuration.Backup)
        {
            S3Settings = BackupTask.GetBackupConfigurationFromScript(configuration.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                database, settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForS3(configuration.S3Settings, database.Name), isServerWide),
            AzureSettings = BackupTask.GetBackupConfigurationFromScript(configuration.AzureSettings, x => JsonDeserializationServer.AzureSettings(x),
                database, settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForAzure(configuration.AzureSettings, database.Name), isServerWide),
            GlacierSettings = BackupTask.GetBackupConfigurationFromScript(configuration.GlacierSettings, x => JsonDeserializationServer.GlacierSettings(x),
                database, settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForGlacier(configuration.GlacierSettings, database.Name), isServerWide),
            GoogleCloudSettings = BackupTask.GetBackupConfigurationFromScript(configuration.GoogleCloudSettings, x => JsonDeserializationServer.GoogleCloudSettings(x),
                database, settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForGoogleCloud(configuration.GoogleCloudSettings, database.Name), isServerWide),
            FtpSettings = BackupTask.GetBackupConfigurationFromScript(configuration.FtpSettings, x => JsonDeserializationServer.FtpSettings(x),
                database, settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForFtp(configuration.FtpSettings, database.Name), isServerWide),
            DatabaseName = database.Name,
            TaskName = taskName,
            BackupType = configuration.BackupType,
            Destination = destination,
            OnBackupException = backupException
        };
    }
}
