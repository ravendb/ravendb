using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.ServerWide;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.PeriodicBackup;

public abstract class FileUploaderBase : FileUploaderDownloaderBase
{
    protected readonly bool _isFullBackup;
    protected readonly Action<IOperationProgress> _onProgress;
    protected readonly RetentionPolicyBaseParameters _retentionPolicyParameters;
    protected readonly BackupResult _backupResult;
    protected readonly RavenLogger _logger;

    protected const string AzureName = "Azure";
    protected const string S3Name = "S3";
    protected const string GlacierName = "Glacier";
    protected const string GoogleCloudName = "Google Cloud";
    protected const string FtpName = "FTP";

    protected FileUploaderBase(UploaderSettings settings, RetentionPolicyBaseParameters retentionPolicyParameters, RavenLogger logger, BackupResult backupResult, Action<IOperationProgress> onProgress, OperationCancelToken taskCancelToken) : base(settings, taskCancelToken)
    {
        _onProgress = onProgress;
        _backupResult = backupResult;
        _retentionPolicyParameters = retentionPolicyParameters;
        _isFullBackup = retentionPolicyParameters?.IsFullBackup ?? false;
        _logger = logger;
    }

    protected void AddInfo(string message)
    {
        _backupResult.AddInfo(message);
        _onProgress.Invoke(_backupResult.Progress);
    }

    protected void DeleteFromS3(S3Settings settings)
    {
        using (var client = new RavenAwsS3Client(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, _settings.FolderName, _settings.FileName);
            client.DeleteObject(key);

            if (_logger.IsInfoEnabled)
                _logger.Info($"{ReportDeletion(S3Name)} bucket named: {settings.BucketName}, with key: {key}");
        }
    }

    protected void DeleteFromAzure(AzureSettings settings)
    {
        using (var client = RavenAzureClient.Create(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, _settings.FolderName, _settings.FileName);
            client.DeleteBlobs(new List<string> { key });

            if (_logger.IsInfoEnabled)
                _logger.Info($"{ReportDeletion(AzureName)} container: {settings.StorageContainer}, with key: {key}");
        }
    }

    protected void DeleteFromGoogleCloud(GoogleCloudSettings settings)
    {
        using (var client = new RavenGoogleCloudClient(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, _settings.FolderName, _settings.FileName);
            client.DeleteObject(key);

            if (_logger.IsInfoEnabled)
                _logger.Info($"{ReportDeletion(GoogleCloudName)} storage bucket: {settings.BucketName}");
        }
    }
    protected async Task<IDictionary<string, string>> GetObjectMetadataFromS3Async(S3Settings settings, string folderName, string fileName)
    {
        using (var client = new RavenAwsS3Client(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
            return await client.GetObjectMetadataAsync(key);
        }
    }

    protected async Task<IDictionary<string, string>> GetObjectMetadataFromAzureAsync(AzureSettings settings, string folderName, string fileName)
    {
        using (IRavenAzureClient client = RavenAzureClient.Create(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
            return await client.GetObjectMetadataAsync(key);
        }
    }

    protected async Task<long?> GetObjectSizeS3Async(string folderName, string objKeyName)
    {
        IDictionary<string, string> metadata = await GetObjectMetadataFromS3Async(_settings.S3Settings, folderName, objKeyName);
        return GetObjectSizeFromMetadataInternal(metadata);
    }

    protected async Task<long?> GetObjectSizeAzureAsync(string folderName, string objKeyName)
    {
        IDictionary<string, string> metadata = await GetObjectMetadataFromAzureAsync(_settings.AzureSettings, folderName, objKeyName);
        return GetObjectSizeFromMetadataInternal(metadata);
    }

    private long? GetObjectSizeFromMetadataInternal(IDictionary<string, string> metadata)
    {
        if (metadata != null && metadata.TryGetValue(Constants.Headers.ContentLength, out var contentLengthStr) &&
            long.TryParse(contentLengthStr, out var cloudSize))
        {
            return cloudSize;
        }

        return null;
    }

    private string ReportDeletion(string name)
    {
        return $"Successfully deleted backup file '{_settings.FileName}' to {name}";
    }

    public static BackupResult GenerateUploadResult()
    {
        return new BackupResult
        {
            // Skipped will be set later, if needed
            S3Backup = new UploadToS3
            {
                Skipped = true
            },
            AzureBackup = new UploadToAzure
            {
                Skipped = true
            },
            GoogleCloudBackup = new UploadToGoogleCloud
            {
                Skipped = true
            },
            GlacierBackup = new UploadToGlacier
            {
                Skipped = true
            },
            FtpBackup = new UploadToFtp
            {
                Skipped = true
            }
        };
    }
}
