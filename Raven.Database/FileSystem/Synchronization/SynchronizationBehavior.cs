// -----------------------------------------------------------------------
//  <copyright file="SynchronizationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Storage;
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

		public SynchronizationReport Execute()
		{
			var report = new SynchronizationReport(fileName, sourceFileEtag, type);
			
			try
			{
				AssertOperationAndLockFile();

				NotifyStart();

				Cleanup();

				var localMetadata = fs.Synchronizations.GetLocalMetadata(fileName);

				bool conflictResolved;
				AssertConflictDetection(localMetadata, out conflictResolved);

				switch (type)
				{
					case SynchronizationType.Delete:
						ExecuteDelete(localMetadata);
						break;
					case SynchronizationType.Rename:
						ExecuteRename(Rename);
						break;
					default:
						throw new ArgumentOutOfRangeException("type", type.ToString());
				}

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

		private void AssertOperationAndLockFile()
		{
			fs.Storage.Batch(accessor =>
			{
				fs.Synchronizations.AssertFileIsNotBeingSynced(fileName);

				//switch (type)
				//{TODO arek
				//	case SynchronizationType.Delete:
				//		fs.Files.AssertDeleteOperationNotVetoed(fileName);
				//		break;
				//	default:
				//		throw new ArgumentOutOfRangeException("type", type.ToString());
				//}

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

		private void Cleanup()
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

				var tombstoneMetadata = new RavenJObject
                                                    {
                                                        {
                                                            SynchronizationConstants.RavenSynchronizationHistory,
                                                            localMetadata[SynchronizationConstants.RavenSynchronizationHistory]
                                                        },
                                                        {
                                                            SynchronizationConstants.RavenSynchronizationVersion,
                                                            localMetadata[SynchronizationConstants.RavenSynchronizationVersion]
                                                        },
                                                        {
                                                            SynchronizationConstants.RavenSynchronizationSource,
                                                            localMetadata[SynchronizationConstants.RavenSynchronizationSource]
                                                        }
                                                    }.WithDeleteMarker();

				fs.Historian.UpdateLastModified(tombstoneMetadata);
				accessor.PutFile(fileName, 0, tombstoneMetadata, true);
			});
		}

		private static bool ShouldAddExceptionToReport(Exception ex)
		{
			return ex is ConflictResolvedInFavourOfCurrentVersionException == false;
		}
	}
}