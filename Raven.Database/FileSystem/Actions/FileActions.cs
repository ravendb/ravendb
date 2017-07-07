// -----------------------------------------------------------------------
//  <copyright file="FileActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Storage.Exceptions;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Actions
{
    public class FileActions : ActionsBase, IDisposable
    {
        internal const int MaxNumberOfFilesToDeleteByCleanupTaskRun = 1024;

        private readonly ConcurrentDictionary<string, Task> deleteFileTasks = new ConcurrentDictionary<string, Task>();
        private readonly ConcurrentDictionary<string, Task> renameFileTasks = new ConcurrentDictionary<string, Task>();
        private readonly ConcurrentDictionary<string, FileHeader> uploadingFiles = new ConcurrentDictionary<string, FileHeader>();
        private readonly SemaphoreSlim maxNumberOfConcurrentDeletionsInBackground = new SemaphoreSlim(10);

        public FileActions(RavenFileSystem fileSystem, ILog log)
            : base(fileSystem, log)
        {
            InitializeTimer();
        }

        private void InitializeTimer()
        {
            FileSystem.TimerManager.NewTimer(state =>
            {
                ResumeFileRenamingAsync();
                CleanupDeletedFilesAsync();
            }, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(15));
        }

        public async Task PutAsync(string name, Etag etag, RavenJObject metadata, Func<Task<Stream>> streamAsync, PutOperationOptions options)
        {
            FileUpdateResult putResult = null;

            try
            {
                FileSystem.MetricsCounters.FilesPerSecond.Mark();

                name = FileHeader.Canonize(name);

                if (options.PreserveTimestamps)
                {
                    if (!metadata.ContainsKey(Constants.RavenCreationDate))
                    {
                        if (metadata.ContainsKey(Constants.CreationDate))
                            metadata[Constants.RavenCreationDate] = metadata[Constants.CreationDate];
                        else
                            throw new InvalidOperationException("Preserve Timestamps requires that the client includes the Raven-Creation-Date header.");
                    }

                    Historian.UpdateLastModified(metadata, options.LastModified.HasValue ? options.LastModified.Value : DateTimeOffset.UtcNow);
                }
                else
                {
                    metadata[Constants.RavenCreationDate] = DateTimeOffset.UtcNow;

                    Historian.UpdateLastModified(metadata);
                }

                // TODO: To keep current filesystems working. We should remove when adding a new migration. 
                metadata[Constants.CreationDate] = metadata[Constants.RavenCreationDate].Value<DateTimeOffset>().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);

                Historian.Update(name, metadata);

                long? size = -1;

                Storage.Batch(accessor =>
                {
                    FileSystem.Synchronizations.AssertFileIsNotBeingSynced(name);
                    AssertPutOperationNotVetoed(name, metadata);

                    SynchronizationTask.Cancel(name);

                    var contentLength = options.ContentLength;
                    var contentSize = options.ContentSize;

                    if (contentLength == 0 || contentSize.HasValue == false)
                    {
                        size = contentLength;
                        if (options.TransferEncodingChunked)
                            size = null;
                    }
                    else
                    {
                        size = contentSize;
                    }

                    FileSystem.PutTriggers.Apply(trigger => trigger.OnPut(name, metadata));

                    using (FileSystem.DisableAllTriggersForCurrentThread())
                    {
                        IndicateFileToDelete(name, etag);
                    }

                    putResult = accessor.PutFile(name, size, metadata);

                    FileSystem.PutTriggers.Apply(trigger => trigger.AfterPut(name, size, metadata));

                    Search.Index(name, metadata, putResult.Etag);
                });

                Log.Debug("Inserted a new file '{0}' with ETag {1}", name, putResult.Etag);

                using (var contentStream = await streamAsync().ConfigureAwait(false))
                using (var readFileToDatabase = new ReadFileToDatabase(BufferPool, Storage, FileSystem.PutTriggers, contentStream, name, metadata))
                {
                    await readFileToDatabase.Execute().ConfigureAwait(false);

                    if (size != null && readFileToDatabase.TotalSizeRead != size)
                    {
                        throw new HttpResponseException(HttpStatusCode.BadRequest);
                    }

                    if (options.PreserveTimestamps == false)
                        Historian.UpdateLastModified(metadata); // update with the final file size.

                    Log.Debug("File '{0}' was uploaded. Starting to update file metadata and indexes", name);

                    metadata["Content-MD5"] = readFileToDatabase.FileHash;

                    FileUpdateResult updateMetadata = null;
                    Storage.Batch(accessor => updateMetadata = accessor.UpdateFileMetadata(name, metadata, null));

                    long totalSizeRead = readFileToDatabase.TotalSizeRead;
                    metadata["Content-Length"] = totalSizeRead.ToString(CultureInfo.InvariantCulture);

                    Search.Index(name, metadata, updateMetadata.Etag);
                    Publisher.Publish(new FileChangeNotification { Action = FileChangeAction.Add, File = name });

                    Log.Debug("Updates of '{0}' metadata and indexes were finished. New file ETag is {1}", name, updateMetadata.Etag);
                }
            }
            catch (Exception ex)
            {
                if (putResult != null)
                {
                    using (FileSystem.DisableAllTriggersForCurrentThread())
                    {
                        IndicateFileToDelete(name, null);
                    }
                }

                Log.WarnException(string.Format("Failed to upload a file '{0}'", name), ex);

                throw;
            }
        }

        internal void AssertPutOperationNotVetoed(string name, RavenJObject metadata)
        {
            var vetoResult = FileSystem.PutTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(name, metadata) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("PUT vetoed on file " + name + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
            }
        }

        private void AssertMetadataUpdateOperationNotVetoed(string name, RavenJObject metadata)
        {
            var vetoResult = FileSystem.MetadataUpdateTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowUpdate(name, metadata) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("POST vetoed on file " + name + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
            }
        }

        private void AssertRenameOperationNotVetoed(string name, string newName)
        {
            var vetoResult = FileSystem.RenameTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowRename(name, newName) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("PATCH vetoed on file " + name + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
            }
        }

        private void AssertDeleteOperationNotVetoed(string name)
        {
            var vetoResult = FileSystem.DeleteTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(name) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("DELETE vetoed on file " + name + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
            }
        }

        public void UpdateMetadata(string name, RavenJObject metadata, Etag etag)
        {
            FileUpdateResult updateMetadata = null;

            Storage.Batch(accessor =>
            {
                AssertMetadataUpdateOperationNotVetoed(name, metadata);

                Historian.UpdateLastModified(metadata);
                
                FileSystem.MetadataUpdateTriggers.Apply(trigger => trigger.OnUpdate(name, metadata));

                updateMetadata = accessor.UpdateFileMetadata(name, metadata, etag);

                FileSystem.MetadataUpdateTriggers.Apply(trigger => trigger.AfterUpdate(name, metadata));
            });

            Search.Index(name, metadata, updateMetadata.Etag);

            FileSystem.Publisher.Publish(new FileChangeNotification
            {
                File = name,
                Action = FileChangeAction.Update
            });

            Log.Debug("Metadata of a file '{0}' was updated", name);
        }

        public void ExecuteRenameOperation(RenameFileOperation operation)
        {
            var configName = RavenFileNameHelper.RenameOperationConfigNameForFile(operation.Name);

            Storage.Batch(accessor =>
            {
                AssertRenameOperationNotVetoed(operation.Name, operation.Rename);

                Publisher.Publish(new FileChangeNotification { File = operation.Name, Action = FileChangeAction.Renaming });

                var previousRenameTombstone = accessor.ReadFile(operation.Rename);

                if (previousRenameTombstone != null &&
                    previousRenameTombstone.Metadata[SynchronizationConstants.RavenDeleteMarker] != null)
                {
                    // if there is a tombstone delete it
                    accessor.Delete(previousRenameTombstone.FullPath);
                }

                FileSystem.RenameTriggers.Apply(trigger => trigger.OnRename(operation.Name, operation.MetadataAfterOperation));

                accessor.RenameFile(operation.Name, operation.Rename, true);
                accessor.UpdateFileMetadata(operation.Rename, operation.MetadataAfterOperation, null);

                FileSystem.RenameTriggers.Apply(trigger => trigger.AfterRename(operation.Name, operation.Rename, operation.MetadataAfterOperation));

                // copy renaming file metadata and set special markers
                var tombstoneMetadata = new RavenJObject(operation.MetadataAfterOperation).WithRenameMarkers(operation.Rename);

                accessor.PutFile(operation.Name, 0, tombstoneMetadata, true); // put rename tombstone

                // let's bump renamed doc etag so it'll be greater than tombstone
                var touchResult = accessor.TouchFile(operation.Rename, null);

                accessor.DeleteConfig(configName);

                Search.Delete(operation.Name);
                Search.Index(operation.Rename, operation.MetadataAfterOperation, touchResult.Etag);
            });

            Publisher.Publish(new ConfigurationChangeNotification { Name = configName, Action = ConfigurationChangeAction.Set });
            Publisher.Publish(new FileChangeNotification { File = operation.Rename, Action = FileChangeAction.Renamed });
        }

        public void IndicateFileToDelete(string fileName, Etag etag)
        {
            var deletingFileName = RavenFileNameHelper.DeletingFileName(fileName);
            var fileExists = true;

            Storage.Batch(accessor =>
            {
                AssertDeleteOperationNotVetoed(fileName);

                var existingFile = accessor.ReadFile(fileName);

                if (existingFile == null)
                {
                    // do nothing if file does not exist
                    fileExists = false;
                    return;
                }

                if (existingFile.Metadata[SynchronizationConstants.RavenDeleteMarker] != null)
                {
                    // if it is a tombstone drop it
                    accessor.Delete(fileName);
                    fileExists = false;
                    return;
                }

                if (etag != null && existingFile.Etag != etag)
                    throw new ConcurrencyException("Operation attempted on file '" + fileName + "' using a non current etag")
                    {
                        ActualETag = existingFile.Etag,
                        ExpectedETag = etag
                    };

                var metadata = new RavenJObject(existingFile.Metadata).WithDeleteMarker();

                var renameSucceeded = false;

                int deleteAttempts = 0;
                do
                {
                    try
                    {
                        accessor.RenameFile(fileName, deletingFileName);
                        renameSucceeded = true;
                    }
                    catch (FileExistsException) // it means that .deleting file was already existed
                    {
                        var deletingFileHeader = accessor.ReadFile(deletingFileName);

                        if (deletingFileHeader != null && deletingFileHeader.Equals(existingFile))
                        {
                            fileExists = false; // the same file already marked as deleted no need to do it again
                            return;
                        }

                        if (deleteAttempts++ > 128)
                        {
                            Log.Warn("Could not rename a file '{0}' when a delete operation was performed", fileName);
                            throw;
                        }

                        // we need to use different name to do a file rename
                        deletingFileName = RavenFileNameHelper.DeletingFileName(fileName, RandomProvider.GetThreadRandom().Next());
                    }
                } while (renameSucceeded == false);

                accessor.UpdateFileMetadata(deletingFileName, metadata, null);
                accessor.DecrementFileCount(deletingFileName);

                Log.Debug("File '{0}' was renamed to '{1}' and marked as deleted", fileName, deletingFileName);

                var configName = RavenFileNameHelper.DeleteOperationConfigNameForFile(deletingFileName);
                var operation = new DeleteFileOperation { OriginalFileName = fileName, CurrentFileName = deletingFileName };
                accessor.SetConfig(configName, JsonExtensions.ToJObject(operation));

                FileSystem.DeleteTriggers.Apply(trigger => trigger.AfterDelete(fileName));

                Publisher.Publish(new ConfigurationChangeNotification { Name = configName, Action = ConfigurationChangeAction.Set });
                Publisher.Publish(new FileChangeNotification { File = fileName, Action = FileChangeAction.Delete });

                Log.Debug("File '{0}' was deleted", fileName);
            });

            if (fileExists)
            {
                Search.Delete(fileName);
                Search.Delete(deletingFileName);
            }
        }

        public void PutTombstone(string fileName, RavenJObject metadata)
        {
            Storage.Batch(accessor =>
            {
                var tombstoneMetadata = new RavenJObject
                {
                    {
                        SynchronizationConstants.RavenSynchronizationHistory,
                        metadata[SynchronizationConstants.RavenSynchronizationHistory]
                    },
                    {
                        SynchronizationConstants.RavenSynchronizationVersion,
                        metadata[SynchronizationConstants.RavenSynchronizationVersion]
                    },
                    {
                        SynchronizationConstants.RavenSynchronizationSource,
                        metadata[SynchronizationConstants.RavenSynchronizationSource]
                    }
                }.WithDeleteMarker();

                Historian.UpdateLastModified(tombstoneMetadata);
                accessor.PutFile(fileName, 0, tombstoneMetadata, true);
            });
        }

        public Task CleanupDeletedFilesAsync()
        {
            if (maxNumberOfConcurrentDeletionsInBackground.CurrentCount == 0)
                return new CompletedTask();

            var filesToDelete = new List<DeleteFileOperation>();

            Storage.Batch(accessor => filesToDelete = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.DeleteOperationConfigPrefix, 0, MaxNumberOfFilesToDeleteByCleanupTaskRun)
                                                              .Select(config => config.JsonDeserialization<DeleteFileOperation>())
                                                              .ToList());

            if (filesToDelete.Count == 0)
                return new CompletedTask();

            var tasks = new List<Task>();

            foreach (var fileToDelete in filesToDelete)
            {
                var deletingFileName = fileToDelete.CurrentFileName;

                if (IsDeleteInProgress(deletingFileName))
                    continue;

                if (IsUploadInProgress(fileToDelete.OriginalFileName))
                    continue;

                if (IsSynchronizationInProgress(fileToDelete.OriginalFileName))
                    continue;

                if (fileToDelete.OriginalFileName.EndsWith(RavenFileNameHelper.DownloadingFileSuffix)) // if it's .downloading file
                {
                    if (IsSynchronizationInProgress(SynchronizedFileName(fileToDelete.OriginalFileName))) // and file is being synced
                        continue;
                }

                Log.Debug("Starting to delete file '{0}' from storage", deletingFileName);

                var deleteTask = new Task(() =>
                {
                    try
                    {
                        Storage.Batch(accessor => accessor.Delete(deletingFileName));
                    }
                    catch (Exception e)
                    {
                        var warnMessage = string.Format("Could not delete file '{0}' from storage", deletingFileName);

                        Log.Warn(warnMessage, e);

                        throw new InvalidOperationException(warnMessage, e);
                    }
                    var configName = RavenFileNameHelper.DeleteOperationConfigNameForFile(deletingFileName);

                    Storage.Batch(accessor => accessor.DeleteConfig(configName));

                    Publisher.Publish(new ConfigurationChangeNotification
                    {
                        Name = configName,
                        Action = ConfigurationChangeAction.Delete
                    });

                    Log.Debug("File '{0}' was deleted from storage", deletingFileName);
                });

                deleteTask.ContinueWith(x =>
                {
                    Task _;
                    deleteFileTasks.TryRemove(deletingFileName, out _);

                    maxNumberOfConcurrentDeletionsInBackground.Release();
                });

                maxNumberOfConcurrentDeletionsInBackground.Wait();

                deleteTask.Start();

                deleteFileTasks.AddOrUpdate(deletingFileName, deleteTask, (file, oldTask) => deleteTask);

                tasks.Add(deleteTask);
            }

            return Task.WhenAll(tasks);
        }

        public Task ResumeFileRenamingAsync()
        {
            var filesToRename = new List<RenameFileOperation>();

            Storage.Batch(accessor =>
            {
                var renameOpConfigs = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.RenameOperationConfigPrefix, 0, 10);

                filesToRename = renameOpConfigs.Select(config => config.JsonDeserialization<RenameFileOperation>()).ToList();
            });

            if (filesToRename.Count == 0)
                return Task.FromResult<object>(null);

            var tasks = new List<Task>();

            foreach (var item in filesToRename)
            {
                var renameOperation = item;

                if (IsRenameInProgress(renameOperation.Name))
                    continue;

                FileHeader existingFile = null;

                Storage.Batch(accessor => existingFile = accessor.ReadFile(renameOperation.Name));

                if (existingFile == null)
                    continue;

                if (renameOperation.Etag != null && renameOperation.Etag != existingFile.Etag)
                {
                    Storage.Batch(accessor => accessor.DeleteConfig(RavenFileNameHelper.RenameOperationConfigNameForFile(renameOperation.Name)));
                    continue;
                }

                Log.Debug("Starting to resume a rename operation of a file '{0}' to '{1}'", renameOperation.Name,
                          renameOperation.Rename);

                var renameTask = Task.Run(() =>
                {
                    try
                    {
                        ExecuteRenameOperation(renameOperation);
                        Log.Debug("File '{0}' was renamed to '{1}'", renameOperation.Name, renameOperation.Rename);

                    }
                    catch (Exception e)
                    {
                        Log.Warn(string.Format("Could not rename file '{0}' to '{1}'", renameOperation.Name, renameOperation.Rename), e);
                        throw;
                    }
                });

                renameFileTasks.AddOrUpdate(renameOperation.Name, renameTask, (file, oldTask) => renameTask);

                tasks.Add(renameTask);
            }

            return Task.WhenAll(tasks);
        }

        private static string SynchronizedFileName(string originalFileName)
        {
            return originalFileName.Substring(0, originalFileName.IndexOf(RavenFileNameHelper.DownloadingFileSuffix, StringComparison.InvariantCulture));
        }

        private bool IsSynchronizationInProgress(string originalFileName)
        {
            if (!FileLockManager.TimeoutExceeded(originalFileName, Storage))
                return true;
            return false;
        }

        private bool IsUploadInProgress(string originalFileName)
        {
            FileHeader deletedFile = null;
            Storage.Batch(accessor => deletedFile = accessor.ReadFile(originalFileName));

            if (deletedFile != null) // if there exists a file already marked as deleted
            {
                if (deletedFile.IsFileBeingUploadedOrUploadHasBeenBroken()) // and might be uploading at the moment
                {
                    if (!uploadingFiles.ContainsKey(deletedFile.FullPath))
                    {
                        uploadingFiles.TryAdd(deletedFile.FullPath, deletedFile);
                        return true; // first attempt to delete a file, prevent this time
                    }
                    var uploadingFile = uploadingFiles[deletedFile.FullPath];
                    if (uploadingFile != null && uploadingFile.UploadedSize != deletedFile.UploadedSize)
                    {
                        return true; // if uploaded size changed it means that file is being uploading
                    }
                    FileHeader header;
                    uploadingFiles.TryRemove(deletedFile.FullPath, out header);
                }
            }
            return false;
        }

        private bool IsDeleteInProgress(string deletingFileName)
        {
            Task existingTask;

            if (deleteFileTasks.TryGetValue(deletingFileName, out existingTask))
            {
                if (!existingTask.IsCompleted)
                {
                    return true;
                }

                deleteFileTasks.TryRemove(deletingFileName, out existingTask);
            }
            return false;
        }

        private bool IsRenameInProgress(string fileName)
        {
            Task existingTask;

            if (renameFileTasks.TryGetValue(fileName, out existingTask))
            {
                if (!existingTask.IsCompleted)
                {
                    return true;
                }

                renameFileTasks.TryRemove(fileName, out existingTask);
            }
            return false;
        }

        public class PutOperationOptions
        {
            public bool PreserveTimestamps { get; set; }

            public DateTimeOffset? LastModified { get; set; }

            public long? ContentLength { get; set; }

            public long? ContentSize { get; set; }

            public bool TransferEncodingChunked { get; set; }
        }

        public void Dispose()
        {
            maxNumberOfConcurrentDeletionsInBackground.Dispose();
        }
    }
}
