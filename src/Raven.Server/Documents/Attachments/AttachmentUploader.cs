using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server.Logging;
using Sparrow.Server.LowMemory;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.Attachments;

public sealed class AttachmentUploader : MultipleFileUploaderBase<AttachmentUploadToCloudHolder>
{
    private readonly short _concurrentThreads;
    private static long _bigAttachmentSize => GetAttachmentSizeThreshold();

    public Action<AttachmentUploadToCloudHolder> OnSuccessAction;
    public Action<AttachmentUploadToCloudHolder> OnExceptionAction;

    public AttachmentUploader(UploaderSettings settings, RavenLogger logger, OperationCancelToken taskCancelToken) :
        base(settings, retentionPolicyParameters: null, logger, backupResult: GenerateUploadResult(), onProgress: progress => { }, taskCancelToken)
    {
        _concurrentThreads = settings.ConcurrentThreads;
    }

    public override string GetBackupDescription()
    {
        return $"{nameof(AttachmentUploader)} for identifier '{_settings.TaskName}'";
    }

    public IDictionary<string, string> GetObjectMetadata(string folderName, string objKeyName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                return GetObjectMetadataFromS3(_settings.S3Settings, folderName, objKeyName);

            case BackupConfiguration.BackupDestination.Azure:
                return GetObjectMetadataFromAzure(_settings.AzureSettings, folderName, objKeyName);

            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    public void CreateUploadTask(DocumentDatabase database, AbstractBackgroundWorkStorage.DocumentExpirationInfo doc, Stream attachmentStream, string objKeyName, long attachmentLength, CancellationToken token)
    {
        Task task = CreateUploadTaskInternal(database, attachmentStream, objKeyName, attachmentLength);
        task.Start();

        _threads.Add(new AttachmentUploadToCloudHolder(task, doc, attachmentLength));
    }

    private Task CreateUploadTaskInternal(DocumentDatabase database, Stream attachmentStream, string objKeyName, long attachmentLength)
    {
        var taskOptions = attachmentLength > _bigAttachmentSize
            ? TaskCreationOptions.LongRunning
            : TaskCreationOptions.RunContinuationsAsynchronously;

        var task = new Task(() =>
        {
            using (attachmentStream)
            using (var stream = StreamForBackupDestination(database, string.Empty, objKeyName))
            {
                if (_logger.IsDebugEnabled)
                {
                    _logger.Debug($"Starting the upload of retired attachment '{objKeyName}' on {GetBackupDescription()}.");
                }

                attachmentStream.CopyTo(stream);

                TaskCancelToken.Token.ThrowIfCancellationRequested();
            }
        },
        TaskCancelToken.Token,
        taskOptions);

        return task;
    }

    public async Task<bool> WaitForFinishedTasksIfNeededAsync(Stopwatch sp, OperationCancelToken token)
    {
        if (_threads.Count < _concurrentThreads)
            return true;

        while (_threads.Count >= _concurrentThreads)
        {
            if (token.Token.IsCancellationRequested)
                return false;

            if (sp.ElapsedMilliseconds > RetireAttachmentsSender.ReadTransactionMaxOpenTimeInMs)
                return false;

            // Create a timeout task for the polling interval
            await Task.Delay(512);

            // Remove all completed tasks (not just the first one found)
            for (int i = _threads.Count - 1; i >= 0; i--)
            {
                if (token.Token.IsCancellationRequested)
                {
                    return false;
                }

                if (AssertTaskStateAndInvokeAction(_threads[i]))
                {
                    _threads.RemoveAt(i);
                }
            }
        }

        if (token.Token.IsCancellationRequested)
        {
            return false;
        }

        return true;
    }

    private bool AssertTaskStateAndInvokeAction(AttachmentUploadToCloudHolder task)
    {
        if (task.UploadTask.IsCompleted)
        {
            if (task.UploadTask.IsCompletedSuccessfully)
            {
                OnSuccessAction?.Invoke(task);
            }
            else if (task.UploadTask.IsFaulted && task.UploadTask.Exception != null)
            {
                if (_logger.IsErrorEnabled)
                {
                    _logger.Error($"Upload task of retired attachment '{task.Doc.LowerId}' with identifier '{task.Doc.Id}' failed.", task.UploadTask.Exception);
                }

                OnExceptionAction?.Invoke(task);
            }

            return true;
        }

        return false;
    }

    public override void Execute()
    {
        foreach (var t in _threads)
        {
            TaskCancelToken.Token.ThrowIfCancellationRequested();

            AsyncHelpers.RunSync(() => t.UploadTask);
            AssertTaskStateAndInvokeAction(t);
        }

        _threads.Clear();
    }

    private static long GetAttachmentSizeThreshold()
    {
        if (PlatformDetails.Is32Bits)
            return 64 * 1024 * 1024;

        if (MemoryInformation.TotalPhysicalMemory >= new Size(64, SizeUnit.Gigabytes)) // 64+ GB RAM
            return 512 * 1024 * 1024; // 512 MB
        else if (MemoryInformation.TotalPhysicalMemory >= new Size(32, SizeUnit.Gigabytes)) // 32+ GB RAM
            return 256 * 1024 * 1024; // 256 MB
        else if (MemoryInformation.TotalPhysicalMemory >= new Size(16, SizeUnit.Gigabytes)) // 16+ GB RAM  
            return 128 * 1024 * 1024; // 128 MB
        else
            return 64 * 1024 * 1024;  // 64 MB for smaller 64-bit systems
    }
}
