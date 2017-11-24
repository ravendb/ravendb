// -----------------------------------------------------------------------
//  <copyright file="SynchronizationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization.Multipart;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Synchronization
{
    public class SynchronizationBehavior
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly string fileName;
        private readonly Etag sourceFileEtag;
        private readonly RavenJObject sourceMetadata;
        private readonly FileSystemInfo sourceFs;
        private readonly SynchronizationType type;
        private readonly RavenFileSystem fs;

        public SynchronizationBehavior(string fileName, Etag sourceFileEtag, RavenJObject sourceMetadata, FileSystemInfo sourceFs, SynchronizationType type, RavenFileSystem fs)
        {
            this.fileName = fileName;
            this.sourceFileEtag = sourceFileEtag;
            this.sourceMetadata = sourceMetadata;
            this.sourceFs = sourceFs;
            this.type = type;
            this.fs = fs;
        }

        public string Rename { get; set; }

        public HttpContent MultipartContent { get; set; }

        public async Task<SynchronizationReport> Execute()
        {
            var report = new SynchronizationReport(fileName, sourceFileEtag, type);
            
            try
            {
                AssertOperationAndLockFile();

                NotifyStart();

                Prepare();

                var localMetadata = fs.Synchronizations.GetLocalMetadata(fileName);

                bool conflictResolved;
                AssertConflictDetection(localMetadata, out conflictResolved);

                fs.SynchronizationTriggers.Apply(trigger => trigger.BeforeSynchronization(fileName, sourceMetadata, type));

                dynamic afterSynchronizationTriggerData = null;

                switch (type)
                {
                    case SynchronizationType.Delete:
                        ExecuteDelete(localMetadata);
                        break;
                    case SynchronizationType.Rename:
                        ExecuteRename(Rename);
                        break;
                    case SynchronizationType.MetadataUpdate:
                        ExecuteMetadataUpdate();
                        break;
                    case SynchronizationType.ContentUpdateNoRDC:
                    case SynchronizationType.ContentUpdate:

                        if (type == SynchronizationType.ContentUpdateNoRDC)
                        {
                            using (fs.DisableAllTriggersForCurrentThread())
                            {
                                fs.Files.IndicateFileToDelete(fileName, null);
                            }

                            localMetadata = null;
                        }

                        await ExecuteContentUpdate(localMetadata, report).ConfigureAwait(false);
                        
                        afterSynchronizationTriggerData = new
                        {
                            TempFileName = RavenFileNameHelper.DownloadingFileName(fileName)
                        };
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("type", type.ToString());
                }


                fs.SynchronizationTriggers.Apply(trigger => trigger.AfterSynchronization(fileName, sourceMetadata, type, afterSynchronizationTriggerData));

                if (conflictResolved)
                {
                    fs.ConflictArtifactManager.Delete(fileName);

                    fs.Publisher.Publish(new ConflictNotification
                    {
                        FileName = fileName,
                        Status = ConflictStatus.Resolved
                    });
                }
            }
            catch (Exception ex)
            {
                if (ShouldAddExceptionToReport(ex))
                {
                    report.Exception = ex;

                    Log.WarnException(string.Format("Error was occurred during {2} synchronization of file '{0}' from {1}", fileName, sourceFs, type), ex);
                }
            }

            fs.Synchronizations.FinishSynchronization(fileName, report, sourceFs, sourceFileEtag);

            NotifyEnd();

            return report;
        }

        private void AssertOperationAndLockFile()
        {
            fs.Storage.Batch(accessor =>
            {
                fs.Synchronizations.AssertFileIsNotBeingSynced(fileName);

                if (type == SynchronizationType.ContentUpdate)
                    fs.Files.AssertPutOperationNotVetoed(fileName, sourceMetadata);

                fs.FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceFs, accessor);
            });
        }

        private void NotifyStart()
        {
            fs.SynchronizationTask.IncomingSynchronizationStarted(fileName, sourceFs, sourceFileEtag, type);

            fs.Publisher.Publish(new SynchronizationUpdateNotification
            {
                FileName = fileName,
                SourceFileSystemUrl = sourceFs.Url,
                SourceServerId = sourceFs.Id,
                Type = type,
                Action = SynchronizationAction.Start,
                Direction = SynchronizationDirection.Incoming
            });
        }

        private void NotifyEnd()
        {
            fs.SynchronizationTask.IncomingSynchronizationFinished(fileName, sourceFs, sourceFileEtag);

            fs.Publisher.Publish(new SynchronizationUpdateNotification
            {
                FileName = fileName,
                SourceFileSystemUrl = sourceFs.Url,
                SourceServerId = sourceFs.Id,
                Type = type,
                Action = SynchronizationAction.Finish,
                Direction = SynchronizationDirection.Incoming
            });
        }

        private void Prepare()
        {
            fs.Storage.Batch(accessor =>
            {
                // remove previous SyncResult
                fs.Synchronizations.DeleteSynchronizationReport(fileName, accessor);

                using (fs.DisableAllTriggersForCurrentThread())
                {
                    // remove previous .downloading file
                    fs.Files.IndicateFileToDelete(RavenFileNameHelper.DownloadingFileName(fileName), null);
                }
            });
        }

        private void AssertConflictDetection(RavenJObject localMetadata, out bool isConflictResolved)
        {
            if (localMetadata == null)
            {
                isConflictResolved = false;
                return;
            }

            var conflict = fs.ConflictDetector.Check(fileName, localMetadata, sourceMetadata, sourceFs.Url);
            if (conflict == null)
            {
                isConflictResolved = false;
                return;
            }

            isConflictResolved = fs.ConflictResolver.CheckIfResolvedByRemoteStrategy(localMetadata, conflict);

            if (isConflictResolved)
                return;

            ConflictResolutionStrategy strategy;
            if (fs.ConflictResolver.TryResolveConflict(fileName, conflict, localMetadata, sourceMetadata, out strategy))
            {
                switch (strategy)
                {
                    case ConflictResolutionStrategy.RemoteVersion:
                        if (Log.IsDebugEnabled)
                            Log.Debug("Conflict automatically resolved by choosing remote version of the file {0}", fileName);
                        return;
                    case ConflictResolutionStrategy.CurrentVersion:

                        fs.Storage.Batch(accessor =>
                        {
                            accessor.UpdateFileMetadata(fileName, localMetadata, null);

                            fs.ConflictArtifactManager.Delete(fileName, accessor);
                        });
                        if (Log.IsDebugEnabled)
                            Log.Debug("Conflict automatically resolved by choosing current version of the file {0}", fileName);

                        throw new ConflictResolvedInFavourOfCurrentVersionException();
                }
            }

            fs.ConflictArtifactManager.Create(fileName, conflict);

            fs.Publisher.Publish(new ConflictNotification
            {
                FileName = fileName,
                SourceServerUrl = sourceFs.Url,
                Status = ConflictStatus.Detected,
                RemoteFileHeader = new FileHeader(fileName, localMetadata)
            });
            if (Log.IsDebugEnabled)
                Log.Debug("File '{0}' is in conflict with synchronized version from {1} ({2}). File marked as conflicted, conflict configuration item created",
                fileName, sourceFs.Url, sourceFs.Id);

            throw new SynchronizationException(string.Format("File {0} is conflicted", fileName));
        }

        private void ExecuteDelete(RavenJObject localMetadata)
        {
            if(localMetadata == null) // nothing to do local file does not exists
                return;

            fs.Storage.Batch(accessor =>
            {
                fs.Files.IndicateFileToDelete(fileName, null);
                fs.Files.PutTombstone(fileName, localMetadata);
            });
        }

        private void ExecuteMetadataUpdate()
        {
            fs.Files.UpdateMetadata(fileName, sourceMetadata, null);
        }

        private void ExecuteRename(string rename)
        {
            Etag currentEtag = null;

            fs.Storage.Batch(accessor =>
            {
                currentEtag = accessor.ReadFile(fileName).Etag;
            });

            fs.Files.ExecuteRenameOperation(new RenameFileOperation(fileName, rename, currentEtag, sourceMetadata.DropRenameMarkers())
            {
                ForceExistingFileRemoval = true
            });
        }

        private async Task ExecuteContentUpdate(RavenJObject localMetadata, SynchronizationReport report)
        {
            var tempFileName = RavenFileNameHelper.DownloadingFileName(fileName);

            using (var localFile = localMetadata != null ? StorageStream.Reading(fs.Storage, fileName) : null)
            {
                fs.PutTriggers.Apply(trigger => trigger.OnPut(tempFileName, sourceMetadata));

                fs.Historian.UpdateLastModified(sourceMetadata);

                var synchronizingFile = SynchronizingFileStream.CreatingOrOpeningAndWriting(fs, tempFileName, sourceMetadata);

                fs.PutTriggers.Apply(trigger => trigger.AfterPut(tempFileName, null, sourceMetadata));

                var provider = new MultipartSyncStreamProvider(synchronizingFile, localFile);

                if (Log.IsDebugEnabled)
                    Log.Debug("Starting to process/read multipart content of a file '{0}'", fileName);

                await MultipartContent.ReadAsMultipartAsync(provider).ConfigureAwait(false);

                if (Log.IsDebugEnabled)
                    Log.Debug("Multipart content of a file '{0}' was processed/read", fileName);

                report.BytesCopied = provider.BytesCopied;
                report.BytesTransfered = provider.BytesTransfered;
                report.NeedListLength = provider.NumberOfFileParts;

                synchronizingFile.PreventUploadComplete = false;
                synchronizingFile.Flush();
                synchronizingFile.Dispose();
                sourceMetadata["Content-MD5"] = synchronizingFile.FileHash;

                FileUpdateResult updateResult = null;
                fs.Storage.Batch(accessor => updateResult = accessor.UpdateFileMetadata(tempFileName, sourceMetadata, null));

                fs.Storage.Batch(accessor =>
                {
                    using (fs.DisableAllTriggersForCurrentThread())
                    {
                        fs.Files.IndicateFileToDelete(fileName, null);
                    }

                    accessor.RenameFile(tempFileName, fileName);

                    fs.Search.Delete(tempFileName);
                    fs.Search.Index(fileName, sourceMetadata, updateResult.Etag);
                });

                if (Log.IsDebugEnabled)
                {
                    var message = localFile == null 
                        ? string.Format("Temporary downloading file '{0}' was renamed to '{1}'. Indexes were updated.", tempFileName, fileName) 
                        : string.Format("Old file '{0}' was deleted. Indexes were updated.", fileName);

                    Log.Debug(message);
                }

                fs.Publisher.Publish(new FileChangeNotification { File = fileName, Action = localFile == null ? FileChangeAction.Add : FileChangeAction.Update });
            }
        }

        private static bool ShouldAddExceptionToReport(Exception ex)
        {
            return ex is ConflictResolvedInFavourOfCurrentVersionException == false;
        }
    }
}
