using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;
using NLog;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Search;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Storage.Exceptions;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Infrastructure
{
	public class StorageOperationsTask
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		private readonly ConcurrentDictionary<string, Task> deleteFileTasks = new ConcurrentDictionary<string, Task>();
		private readonly FileLockManager fileLockManager = new FileLockManager();
		private readonly INotificationPublisher notificationPublisher;
		private readonly ConcurrentDictionary<string, Task> renameFileTasks = new ConcurrentDictionary<string, Task>();
		private readonly IndexStorage search;
		private readonly ITransactionalStorage storage;
		private readonly IObservable<long> timer = Observable.Interval(TimeSpan.FromMinutes(15));
		private readonly ConcurrentDictionary<string, FileHeader> uploadingFiles = new ConcurrentDictionary<string, FileHeader>();

		public StorageOperationsTask(ITransactionalStorage storage, IndexStorage search, INotificationPublisher notificationPublisher)
		{
			this.storage = storage;
			this.search = search;
			this.notificationPublisher = notificationPublisher;

			InitializeTimer();
		}

		private void InitializeTimer()
		{
			timer.Subscribe(tick =>
			{
				ResumeFileRenamingAsync();
				CleanupDeletedFilesAsync();
			});
		}

		public void RenameFile(RenameFileOperation operation)
		{
			var configName = RavenFileNameHelper.RenameOperationConfigNameForFile(operation.Name);
			notificationPublisher.Publish(new FileChange
			{
				File = FilePathTools.Cannoicalise(operation.Name),
				Action = FileChangeAction.Renaming
			});

			storage.Batch(accessor =>
			{
				var previousRenameTombstone = accessor.ReadFile(operation.Rename);

				if (previousRenameTombstone != null &&
					previousRenameTombstone.Metadata[SynchronizationConstants.RavenDeleteMarker] != null)
				{
					// if there is a tombstone delete it
					accessor.Delete(previousRenameTombstone.Name);
				}

				accessor.RenameFile(operation.Name, operation.Rename, true);
				accessor.UpdateFileMetadata(operation.Rename, operation.MetadataAfterOperation);

				// copy renaming file metadata and set special markers
				var tombstoneMetadata =
					new NameValueCollection(operation.MetadataAfterOperation).WithRenameMarkers(operation.Rename);

				accessor.PutFile(operation.Name, 0, tombstoneMetadata, true); // put rename tombstone

				accessor.DeleteConfig(configName);

				search.Delete(operation.Name);
				search.Index(operation.Rename, operation.MetadataAfterOperation);
			});

			notificationPublisher.Publish(new ConfigChange { Name = configName, Action = ConfigChangeAction.Set });
			notificationPublisher.Publish(new FileChange
			{
				File = FilePathTools.Cannoicalise(operation.Rename),
				Action = FileChangeAction.Renamed
			});
		}

		public void IndicateFileToDelete(string fileName)
		{
			var deletingFileName = RavenFileNameHelper.DeletingFileName(fileName);
			var fileExists = true;

			storage.Batch(accessor =>
			{
				var existingFileHeader = accessor.ReadFile(fileName);

				if (existingFileHeader == null)
				{
					// do nothing if file does not exist
					fileExists = false;
					return;
				}

				if (existingFileHeader.Metadata[SynchronizationConstants.RavenDeleteMarker] != null)
				{
					// if it is a tombstone drop it
					accessor.Delete(fileName);
					fileExists = false;
					return;
				}

				var metadata = new NameValueCollection(existingFileHeader.Metadata).WithDeleteMarker();

				var renameSucceeded = false;

				var deleteVersion = 0;

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

						if (deletingFileHeader != null && deletingFileHeader.Equals(existingFileHeader))
						{
							fileExists = false; // the same file already marked as deleted no need to do it again
							return;
						}

						// we need to use different name to do a file rename
						deleteVersion++;
						deletingFileName = RavenFileNameHelper.DeletingFileName(fileName, deleteVersion);
					}
				} while (!renameSucceeded && deleteVersion < 128);

				if (renameSucceeded)
				{
					accessor.UpdateFileMetadata(deletingFileName, metadata);
					accessor.DecrementFileCount(deletingFileName);

					Log.Debug(string.Format("File '{0}' was renamed to '{1}' and marked as deleted",
											fileName, deletingFileName));

					var configName = RavenFileNameHelper.DeleteOperationConfigNameForFile(deletingFileName);
					accessor.SetConfig(configName,
									   new DeleteFileOperation { OriginalFileName = fileName, CurrentFileName = deletingFileName }.
										   AsConfig());

					notificationPublisher.Publish(new ConfigChange { Name = configName, Action = ConfigChangeAction.Set });
				}
				else
				{
					Log.Warn("Could not rename a file '{0}' when a delete operation was performed",
							 fileName);
				}
			});

			if (fileExists)
			{
				search.Delete(fileName);
				search.Delete(deletingFileName);
			}
		}

		public Task CleanupDeletedFilesAsync()
		{
			var filesToDelete = new List<DeleteFileOperation>();

			storage.Batch(
				accessor =>
				filesToDelete =
				accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.DeleteOperationConfigPrefix, 0, 10).Select(
					config => config.AsObject<DeleteFileOperation>()).ToList());

			if(filesToDelete.Count == 0)
				return Task.FromResult<object>(null);

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

				var deleteTask = Task.Run(() =>
				{
					try
					{
						ConcurrencyAwareExecutor.Execute(() => storage.Batch(accessor => accessor.Delete(deletingFileName)), retries: 1);
					}
					catch (Exception e)
					{
						Log.WarnException(string.Format("Could not delete file '{0}' from storage", deletingFileName), e);
						return;
					}
					var configName = RavenFileNameHelper.DeleteOperationConfigNameForFile(deletingFileName);

					storage.Batch(accessor => accessor.DeleteConfig(configName));

					notificationPublisher.Publish(new ConfigChange
					{
						Name = configName,
						Action = ConfigChangeAction.Delete
					});

					Log.Debug("File '{0}' was deleted from storage", deletingFileName);
				});

				deleteFileTasks.AddOrUpdate(deletingFileName, deleteTask, (file, oldTask) => deleteTask);

				tasks.Add(deleteTask);
			}

			return Task.WhenAll(tasks);
		}

		public Task ResumeFileRenamingAsync()
		{
			var filesToRename = new List<RenameFileOperation>();

			storage.Batch(
				accessor =>
				{
					var renameOpConfigs =
						accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.RenameOperationConfigPrefix, 0, 10);

					filesToRename = renameOpConfigs.Select(config => config.AsObject<RenameFileOperation>()).ToList();
				});

			if (filesToRename.Count == 0)
				return Task.FromResult<object>(null);

			var tasks = new List<Task>();

			foreach (var item in filesToRename)
			{
				var renameOperation = item;

				if (IsRenameInProgress(renameOperation.Name))
					continue;

				Log.Debug("Starting to resume a rename operation of a file '{0}' to '{1}'", renameOperation.Name,
						  renameOperation.Rename);

				var renameTask = Task.Run(() =>
				{
					try
					{
						ConcurrencyAwareExecutor.Execute(() => RenameFile(renameOperation), retries: 1);
						Log.Debug("File '{0}' was renamed to '{1}'", renameOperation.Name, renameOperation.Rename);

					}
					catch (Exception e)
					{
						Log.WarnException(
							string.Format("Could not rename file '{0}' to '{1}'", renameOperation.Name, renameOperation.Rename), e);
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
			return originalFileName.Substring(0,
											  originalFileName.IndexOf(RavenFileNameHelper.DownloadingFileSuffix,
																	   StringComparison.InvariantCulture));
		}

		private bool IsSynchronizationInProgress(string originalFileName)
		{
			if (!fileLockManager.TimeoutExceeded(originalFileName, storage))
				return true;
			return false;
		}

		private bool IsUploadInProgress(string originalFileName)
		{
			FileHeader deletedFile = null;
			storage.Batch(accessor => deletedFile = accessor.ReadFile(originalFileName));

			if (deletedFile != null) // if there exists a file already marked as deleted
			{
				if (deletedFile.IsFileBeingUploadedOrUploadHasBeenBroken()) // and might be uploading at the moment
				{
					if (!uploadingFiles.ContainsKey(deletedFile.Name))
					{
						uploadingFiles.TryAdd(deletedFile.Name, deletedFile);
						return true; // first attempt to delete a file, prevent this time
					}
					var uploadingFile = uploadingFiles[deletedFile.Name];
					if (uploadingFile != null && uploadingFile.UploadedSize != deletedFile.UploadedSize)
					{
						return true; // if uploaded size changed it means that file is being uploading
					}
					FileHeader header;
					uploadingFiles.TryRemove(deletedFile.Name, out header);
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
	}
}
