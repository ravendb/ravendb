using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup;

public abstract class BackupUploaderBase
{
    public readonly OperationCancelToken TaskCancelToken;

    protected readonly bool _isFullBackup;
    protected readonly Action<IOperationProgress> _onProgress;
    protected readonly RetentionPolicyBaseParameters _retentionPolicyParameters;
    protected readonly BackupResult _backupResult;
    protected readonly UploaderSettings _settings;
    protected readonly Logger _logger;
    protected readonly List<PoolOfThreads.LongRunningWork> _threads;
    protected readonly ConcurrentSet<Exception> _exceptions;

    protected const string AzureName = "Azure";
    protected const string S3Name = "S3";
    protected const string GlacierName = "Glacier";
    protected const string GoogleCloudName = "Google Cloud";
    protected const string FtpName = "FTP";

    protected BackupUploaderBase(UploaderSettings settings, RetentionPolicyBaseParameters retentionPolicyParameters, Logger logger, BackupResult backupResult,
        Action<IOperationProgress> onProgress, OperationCancelToken taskCancelToken)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _onProgress = onProgress;
        _backupResult = backupResult;

        _retentionPolicyParameters = retentionPolicyParameters;

        _isFullBackup = retentionPolicyParameters?.IsFullBackup ?? false;

        _logger = logger;
        TaskCancelToken = taskCancelToken;
        _threads = new List<PoolOfThreads.LongRunningWork>();
        _exceptions = new ConcurrentSet<Exception>();
    }

    public virtual string CombinePathAndKey(string path, string folderName, string fileName)
    {
        if (path?.EndsWith('/') == true)
            path = path[..^1];

        var prefix = string.IsNullOrWhiteSpace(path) == false ? $"{path}/" : string.Empty;

        return $"{prefix}{folderName}/{fileName}";
    }

    public virtual string GetBackupDescription(BackupType? backupType, bool isFullBackup)
    {
        var fullBackupText = backupType == BackupType.Backup ? "Full backup" : "A snapshot";
        return isFullBackup ? fullBackupText : "Incremental backup";
    }

    protected void AddInfo(string message)
    {
        _backupResult.AddInfo(message);
        _onProgress.Invoke(_backupResult.Progress);
    }

    protected void Execute()
    {
        _threads.ForEach(x => x.Join(int.MaxValue));

        if (_exceptions.IsEmpty == false)
        {
            Console.WriteLine("Execute: _exceptions.IsEmpty == false");
            if (_exceptions.Count == 1)
                throw _exceptions.First();

            if (_exceptions.All(x => x is OperationCanceledException))
                throw _exceptions.First();

            throw new AggregateException(_exceptions);
        }
    }

    protected void CreateDeletionTaskIfNeeded<T>(T settings, Action<T, string,string> deleteFromServer, string targetName, string folderName, string fileName)
        where T : BackupSettings
    {
        if (BackupConfiguration.CanBackupUsing(settings) == false)
            return;

        var threadName = $"Delete backup file of database '{_settings.DatabaseName}' from {targetName} (task: '{_settings.TaskName}')";
        var thread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ =>
        {
            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, _logger);
                NativeMemory.EnsureRegistered();

                AddInfo($"Starting the delete of backup file from {targetName}.");
                deleteFromServer(settings, folderName, fileName);
            }
            catch (Exception e)
            {
                var extracted = e.ExtractSingleInnerException();
                var error = $"Failed to delete the backup file from {targetName}.";
                Exception exception = null;
                if (extracted is OperationCanceledException)
                {
                    // shutting down or HttpClient timeout
                    exception = TaskCancelToken.Token.IsCancellationRequested ? extracted : new TimeoutException(error, e);
                }

                _exceptions.Add(exception ?? new InvalidOperationException(error, e));
            }
        }, null, ThreadNames.ForDeleteBackupFile(threadName, _settings.DatabaseName, targetName, _settings.TaskName));

        _threads.Add(thread);
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
