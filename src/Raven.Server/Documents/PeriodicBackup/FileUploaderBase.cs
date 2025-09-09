using System;
using System.Collections.Generic;
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

    protected void DeleteFromS3(S3Settings settings, string folderName, string fileName)
    {
        using (var client = new RavenAwsS3Client(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
            client.DeleteObject(key);

            if (_logger.IsInfoEnabled)
                _logger.Info($"{ReportDeletion(S3Name)} bucket named: {settings.BucketName}, with key: {key}");
        }
    }

    protected void DeleteFromAzure(AzureSettings settings, string folderName, string fileName)
    {
        using (var client = RavenAzureClient.Create(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
            client.DeleteBlobs(new List<string> { key });

            if (_logger.IsInfoEnabled)
                _logger.Info($"{ReportDeletion(AzureName)} container: {settings.StorageContainer}, with key: {key}");
        }
    }

    protected void DeleteFromGoogleCloud(GoogleCloudSettings settings, string folderName, string fileName)
    {
        using (var client = new RavenGoogleCloudClient(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
            client.DeleteObject(key);

            if (_logger.IsInfoEnabled)
                _logger.Info($"{ReportDeletion(GoogleCloudName)} storage bucket: {settings.BucketName}");
        }
    }

    protected IDictionary<string, string> GetObjectMetadataFromS3(S3Settings settings, string folderName, string fileName)
    {
        using (var client = new RavenAwsS3Client(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
            return client.GetObjectMetadata(key);
        }
    }

    protected IDictionary<string, string> GetObjectMetadataFromAzure(AzureSettings settings, string folderName, string fileName)
    {
        using (IRavenAzureClient client = RavenAzureClient.Create(settings, _settings.Configuration, progress: null, TaskCancelToken.Token))
        {
            var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
            return client.GetObjectMetadata(key);
        }
    }

    protected IDictionary<string, string> GetObjectMetadataFromGoogleCloud(GoogleCloudSettings settings, string folderName, string fileName)
    {
        throw new NotImplementedException();
    }

    protected long? GetObjectSizeFromMetadataS3(IDictionary<string, string> metadata)
    {
        return GetObjectSizeFromMetadataInternal(metadata);
    }

    protected long? GetObjectSizeFromMetadataAzure(IDictionary<string, string> metadata)
    {
        return GetObjectSizeFromMetadataInternal(metadata);
    }

    private long? GetObjectSizeFromMetadataInternal(IDictionary<string, string> metadata)
    {
        if (metadata.TryGetValue(Constants.Headers.ContentLength, out var contentLengthStr) &&
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
