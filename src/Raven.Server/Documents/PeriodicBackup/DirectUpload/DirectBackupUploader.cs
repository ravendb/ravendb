using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public sealed class DirectBackupUploader : BackupUploaderBase, IDisposable
{
    private readonly BackupConfiguration.BackupDestination _destination;

    public DirectBackupUploader(UploaderSettings settings, RetentionPolicyBaseParameters retentionPolicyParameters, Logger logger, BackupResult backupResult, Action<IOperationProgress> onProgress, OperationCancelToken taskCancelToken) :
        base(settings, retentionPolicyParameters, logger, backupResult, onProgress, taskCancelToken)
    {
        _destination = settings.Destination;
    }

    internal Stream StreamForBackupDestination(DocumentDatabase database, string folderName, string fileName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                return new AwsS3DirectUploadStream(GetDirectUploadParameters(
                    progress => new RavenAwsS3Client(_settings.S3Settings, database.Configuration.Backup, progress, TaskCancelToken.Token),
                    _settings.S3Settings.RemoteFolderName, folderName, fileName));

            case BackupConfiguration.BackupDestination.Azure:
                return new AzureDirectUploadStream(GetDirectUploadParameters(
                    progress => RavenAzureClient.Create(_settings.AzureSettings, database.Configuration.Backup, progress, TaskCancelToken.Token),
                    _settings.AzureSettings.RemoteFolderName, folderName, fileName));

            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    private DirectUploadStream<T>.Parameters GetDirectUploadParameters<T>(Func<Progress, T> clientFactory, string remoteFolderName, string folderName, string fileName) where T : IDirectUploader
    {
        return new DirectUploadStream<T>.Parameters
        {
            ClientFactory = clientFactory,
            Key = CombinePathAndKey(remoteFolderName, folderName, fileName),
            Metadata = new Dictionary<string, string>
            {
                { "Description", GetBackupDescription(_settings.BackupType, _isFullBackup) }
            },
            IsFullBackup = _isFullBackup,
            RetentionPolicyParameters = _retentionPolicyParameters,
            CloudUploadStatus = _backupResult.S3Backup,
            OnBackupException = _settings.OnBackupException,
            OnProgress = AddInfo
        };
    }

    public override string CombinePathAndKey(string path, string folderName, string fileName)
    {
        return base.CombinePathAndKey(path, folderName, fileName);
    }

    public override string GetBackupDescription(BackupType? backupType, bool isFullBackup)
    {
        // TODO: egor do I want to put some info here? Like attachment metadata? I can pass prepared string here
        return $"{nameof(DirectBackupUploader)}";
    }

    public void AddDelete(string folderName, string fileName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                CreateDeletionTaskIfNeeded(_settings.S3Settings, DeleteFromS3, S3Name, folderName, fileName);
                break;

            case BackupConfiguration.BackupDestination.Azure:
                CreateDeletionTaskIfNeeded(_settings.AzureSettings, DeleteFromAzure, AzureName, folderName, fileName);
                break;

            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    public void CreateUploadTask(DocumentDatabase database, Stream attachmentStream, string folderName, string objKeyName, CancellationToken token)
    {
        var targetName = _destination == BackupConfiguration.BackupDestination.AmazonS3 ? S3Name : AzureName;
        var threadName = $"Upload retired attachment '{objKeyName}' of database '{_settings.DatabaseName}' from {targetName} (task: '{_settings.TaskName}')";
        var thread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ =>
        {
            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, _logger);
                Sparrow.Utils.NativeMemory.EnsureRegistered();

                AddInfo($"Starting the upload of retired attachment '{objKeyName}'.");

                using (var stream = StreamForBackupDestination(database, folderName, objKeyName))
                    attachmentStream.CopyTo(stream);

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
            //TODO: egor create new thread names
        }, null, ThreadNames.ForUploadBackupFile(threadName, _settings.DatabaseName, targetName, _settings.TaskName));

        _threads.Add(thread);
    }

    public bool TryCleanFinishedThreads(Stopwatch sp, OperationCancelToken token)
    {
        if (_threads.Count < 8)
            return true;

        var cleaned = false;
        while (cleaned == false)
        {
            if (token.Token.IsCancellationRequested)
                return false;

            if (sp.ElapsedMilliseconds > RetireAttachmentsSender.ReadTransactionMaxOpenTimeInMs)
            {
                return false;
            }

            for (int i = _threads.Count - 1; i >= 0; i--)
            {
                if (_threads[i].Join(128))
                {
                    cleaned = true;
                    _threads.RemoveAt(i);
                }

            }
        }

        if (_exceptions.IsEmpty == false)
        {
            // we should rethrow the actual exceptions when we will dispose this class.
            token.Cancel();
            return false;
        }

        return true;
    }

    public void Dispose()
    {
        Execute();
        _threads.Clear();
    }

    public void Reset()
    {
        Execute();
        _threads.Clear();
    }

}
