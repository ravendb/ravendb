using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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

public class AttachmentUploader : MultipleFileUploaderBase<AttachmentUploadToCloudHolder>
{
    private readonly short _concurrentThreads;
    private readonly string _backupDescription;
    private static readonly long _bigAttachmentSizeInBytes = GetAttachmentSizeThreshold();

    public Action<AttachmentUploadToCloudHolder> OnSuccess;
    public Action<AttachmentUploadToCloudHolder> OnException;

    public AttachmentUploader(UploaderSettings settings, RavenLogger logger, OperationCancelToken taskCancelToken) :
        base(settings, retentionPolicyParameters: null, logger, backupResult: GenerateUploadResult(), onProgress: progress => { }, taskCancelToken)
    {
        _concurrentThreads = settings.ConcurrentThreads;
        _backupDescription =  $"{nameof(AttachmentUploader)} for identifier '{_settings.TaskName}'";
    }

    public override string GetBackupDescription()
    {
        return _backupDescription;
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

    public void CreateUploadTask(DocumentDatabase database, AbstractBackgroundWorkStorage.DocumentExpirationInfo doc, Stream attachmentStream, string objKeyName, long attachmentLength)
    {
        Task task = CreateUploadTaskInternal(database, attachmentStream, objKeyName, attachmentLength);
        task.Start();

        _threads.AddLast(new AttachmentUploadToCloudHolder(task, doc, attachmentLength));
    }

    private Task CreateUploadTaskInternal(DocumentDatabase database, Stream attachmentStream, string objKeyName, long attachmentLength)
    {
        var taskOptions = attachmentLength > _bigAttachmentSizeInBytes
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
        if (RetireAttachmentsSender.CanContinueBatch(_logger, sp, totalUploaded: 0, token) == false)
            return false;

        if (_threads.Count < _concurrentThreads)
            return true;

        while (_threads.Count >= _concurrentThreads)
        {
            if (RetireAttachmentsSender.CanContinueBatch(_logger, sp, totalUploaded: 0, token) == false)
                return false;

            // Create a timeout task for the polling interval
            await Task.Delay(512);

            var current = _threads.First;
            while (current != null)
            {
                if (token.Token.IsCancellationRequested)
                    return false;

                var next = current.Next; // Store next before potential removal

                if (AssertTaskStateAndInvokeAction(current.Value))
                {
                    _threads.Remove(current); // O(1) operation for LinkedListNode
                }

                current = next;
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
                OnSuccess?.Invoke(task);
            }
            else if (task.UploadTask.IsFaulted)
            {
                if (_logger.IsErrorEnabled)
                {
                    _logger.Error($"Upload task of retired attachment '{task.Doc.LowerId}' with identifier '{task.Doc.Id}' failed.", task.UploadTask.Exception);
                }

                OnException?.Invoke(task);
            }
            else if (task.UploadTask.IsCanceled)
            {
                if (_logger.IsDebugEnabled)
                {
                    _logger.Debug($"Upload task of retired attachment '{task.Doc.LowerId}' with identifier '{task.Doc.Id}' was canceled.");
                }
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

            try
            {
                AsyncHelpers.RunSync(() => t.UploadTask);
            }
            catch
            {
                // we assert the task state below
            }

            AssertTaskStateAndInvokeAction(t);
        }

        _threads.Clear();
    }

    private static long GetAttachmentSizeThreshold()
    {
        if (PlatformDetails.Is32Bits)
            return 16 * Sparrow.Global.Constants.Size.Megabyte; // 16 MB for smaller 32-bit systems

        if (MemoryInformation.TotalPhysicalMemory >= new Size(64, SizeUnit.Gigabytes)) // 64+ GB RAM
            return 512 * Sparrow.Global.Constants.Size.Megabyte; // 512 MB
        else if (MemoryInformation.TotalPhysicalMemory >= new Size(32, SizeUnit.Gigabytes)) // 32+ GB RAM
            return 256 * Sparrow.Global.Constants.Size.Megabyte; // 256 MB
        else if (MemoryInformation.TotalPhysicalMemory >= new Size(16, SizeUnit.Gigabytes)) // 16+ GB RAM  
            return 128 * Sparrow.Global.Constants.Size.Megabyte; // 128 MB
        else
            return 64 * Sparrow.Global.Constants.Size.Megabyte;  // 64 MB for smaller 64-bit systems
    }
}
