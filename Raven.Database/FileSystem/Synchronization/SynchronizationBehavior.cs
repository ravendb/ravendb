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
		private readonly RavenJObject metadata;
		private readonly FileSystemInfo sourceFs;
		private readonly SynchronizationType type;
		private readonly RavenFileSystem fs;

		public SynchronizationBehavior(string fileName, Etag sourceFileEtag, RavenJObject metadata, FileSystemInfo sourceFs, SynchronizationType type, RavenFileSystem fs)
		{
			this.fileName = fileName;
			this.sourceFileEtag = sourceFileEtag;
			this.metadata = metadata;
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

				fs.SynchronizationTriggers.Apply(trigger => trigger.BeforeSynchronization(fileName, metadata, type));

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
					case SynchronizationType.ContentUpdate:
						await ExecuteContentUpdate(localMetadata, report);
						
						afterSynchronizationTriggerData = new
						{
							TempFileName = RavenFileNameHelper.DownloadingFileName(fileName)
						};
						break;
					default:
						throw new ArgumentOutOfRangeException("type", type.ToString());
				}


				fs.SynchronizationTriggers.Apply(trigger => trigger.AfterSynchronization(fileName, metadata, type, afterSynchronizationTriggerData));

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

					Log.WarnException(string.Format("Error was occurred during deletion synchronization of file '{0}' from {1}", fileName, sourceFs), ex);
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
					fs.Files.AssertPutOperationNotVetoed(fileName, metadata);

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

			var conflict = fs.ConflictDetector.Check(fileName, localMetadata, metadata, sourceFs.Url);
			if (conflict == null)
			{
				isConflictResolved = false;
				return;
			}

			isConflictResolved = fs.ConflictResolver.CheckIfResolvedByRemoteStrategy(localMetadata, conflict);

			if (isConflictResolved)
				return;

			ConflictResolutionStrategy strategy;
			if (fs.ConflictResolver.TryResolveConflict(fileName, conflict, localMetadata, metadata, out strategy))
			{
				switch (strategy)
				{
					case ConflictResolutionStrategy.RemoteVersion:
						Log.Debug("Conflict automatically resolved by choosing remote version of the file {0}", fileName);
						return;
					case ConflictResolutionStrategy.CurrentVersion:

						fs.Storage.Batch(accessor =>
						{
							accessor.UpdateFileMetadata(fileName, localMetadata, null);

							fs.ConflictArtifactManager.Delete(fileName, accessor);
						});

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
			fs.Files.UpdateMetadata(fileName, metadata, null);
		}

		private void ExecuteRename(string rename)
		{
			fs.Files.ExecuteRenameOperation(new RenameFileOperation
			{
				FileSystem = fs.Name,
				Name = fileName,
				Rename = rename,
				MetadataAfterOperation = metadata.DropRenameMarkers()
			});
		}

		private async Task ExecuteContentUpdate(RavenJObject localMetadata, SynchronizationReport report)
		{
			var tempFileName = RavenFileNameHelper.DownloadingFileName(fileName);

			using (var localFile = localMetadata != null ? StorageStream.Reading(fs.Storage, fileName) : null)
			{
				fs.PutTriggers.Apply(trigger => trigger.OnPut(tempFileName, metadata));

				fs.Historian.UpdateLastModified(metadata);

				var synchronizingFile = SynchronizingFileStream.CreatingOrOpeningAndWriting(fs, tempFileName, metadata);

				fs.PutTriggers.Apply(trigger => trigger.AfterPut(tempFileName, null, metadata));

				var provider = new MultipartSyncStreamProvider(synchronizingFile, localFile);

				Log.Debug("Starting to process/read multipart content of a file '{0}'", fileName);

				await MultipartContent.ReadAsMultipartAsync(provider);

				Log.Debug("Multipart content of a file '{0}' was processed/read", fileName);

				report.BytesCopied = provider.BytesCopied;
				report.BytesTransfered = provider.BytesTransfered;
				report.NeedListLength = provider.NumberOfFileParts;

				synchronizingFile.PreventUploadComplete = false;
				synchronizingFile.Flush();
				synchronizingFile.Dispose();
				metadata["Content-MD5"] = synchronizingFile.FileHash;

				MetadataUpdateResult updateResult = null;
				fs.Storage.Batch(accessor => updateResult = accessor.UpdateFileMetadata(tempFileName, metadata, null));

				fs.Storage.Batch(accessor =>
				{
					using (fs.DisableAllTriggersForCurrentThread())
					{
						fs.Files.IndicateFileToDelete(fileName, null);
					}

					accessor.RenameFile(tempFileName, fileName);

					fs.Search.Delete(tempFileName);
					fs.Search.Index(fileName, metadata, updateResult.Etag);
				});

				if (localFile == null)
					Log.Debug("Temporary downloading file '{0}' was renamed to '{1}'. Indexes were updated.", tempFileName, fileName);
				else
					Log.Debug("Old file '{0}' was deleted. Indexes were updated.", fileName);

				fs.Publisher.Publish(new FileChangeNotification { File = fileName, Action = localFile == null ? FileChangeAction.Add : FileChangeAction.Update });
			}
		}

		private static bool ShouldAddExceptionToReport(Exception ex)
		{
			return ex is ConflictResolvedInFavourOfCurrentVersionException == false;
		}
	}
}