using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Logging;
using Raven.Abstractions.RavenFS;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Infrastructure;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Conflictuality;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System.Diagnostics;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class SynchronizationController : RavenFsApiController
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private static readonly ConcurrentDictionary<Guid, ReaderWriterLockSlim> SynchronizationFinishLocks =
			new ConcurrentDictionary<Guid, ReaderWriterLockSlim>();

        [HttpPost]
        [Route("ravenfs/{fileSystemName}/synchronization/ToDestinations")]
        public async Task<HttpResponseMessage> ToDestinations(bool forceSyncingAll)
        {
            var result = await SynchronizationTask.SynchronizeDestinationsAsync(forceSyncingAll);

            return this.GetMessageWithObject(result, HttpStatusCode.OK);
        }

        [HttpPost]
        [Route("ravenfs/{fileSystemName}/synchronization/ToDestination")]
        public async Task<HttpResponseMessage> ToDestination(string destination, bool forceSyncingAll)
        {
            var result = await SynchronizationTask.SynchronizeDestinationAsync(destination + "/ravenfs/" + this.FileSystemName, forceSyncingAll);
            
            return this.GetMessageWithObject(result, HttpStatusCode.OK);
        }

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/synchronization/start/{*fileName}")]
        public async Task<HttpResponseMessage> Start(string fileName)
		{
		    var destination = await ReadJsonObjectAsync<SynchronizationDestination>();

			Log.Debug("Starting to synchronize a file '{0}' to {1}", fileName, destination.FileSystemUrl);

			var result = await SynchronizationTask.SynchronizeFileToAsync(fileName, destination);

            return this.GetMessageWithObject(result, HttpStatusCode.OK);
		}

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/synchronization/MultipartProceed")]
        public async Task<HttpResponseMessage> MultipartProceed()
		{
			if (!Request.Content.IsMimeMultipartContent())
				throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);

            var fileName = Request.Headers.GetValues(SyncingMultipartConstants.FileName).FirstOrDefault();
			var tempFileName = RavenFileNameHelper.DownloadingFileName(fileName);

            var sourceServerInfo = InnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            var sourceFileETag = Guid.Parse(InnerHeaders.GetValues("ETag").First().Trim('\"'));

            var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.ContentUpdate);

			Log.Debug("Starting to process multipart synchronization request of a file '{0}' with ETag {1} from {2}", fileName, sourceFileETag, sourceServerInfo);

			StorageStream localFile = null;
			var isNewFile = false;
			var isConflictResolved = false;

			try
			{
				Storage.Batch(accessor =>
				{
					AssertFileIsNotBeingSynced(fileName, accessor);
					FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceServerInfo, accessor);
				});

				PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Start);

				Storage.Batch(accessor => StartupProceed(fileName, accessor));

                RavenJObject sourceMetadata = GetFilteredMetadataFromHeaders(InnerHeaders); // InnerHeaders.FilterHeadersToObject();

				var localMetadata = GetLocalMetadata(fileName);

                if (localMetadata != null)
                {
                    AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);
                    localFile = StorageStream.Reading(Storage, fileName);
                }
                else
                {
                    isNewFile = true;
                }

                Historian.UpdateLastModified(sourceMetadata);

                var synchronizingFile = SynchronizingFileStream.CreatingOrOpeningAndWritting(Storage, Search, StorageOperationsTask,
                                                                                             tempFileName, sourceMetadata);

                var provider = new MultipartSyncStreamProvider(synchronizingFile, localFile);

                Log.Debug("Starting to process multipart content of a file '{0}'", fileName);

                await Request.Content.ReadAsMultipartAsync(provider);

                Log.Debug("Multipart content of a file '{0}' was processed", fileName);

                report.BytesCopied = provider.BytesCopied;
                report.BytesTransfered = provider.BytesTransfered;
                report.NeedListLength = provider.NumberOfFileParts;

                synchronizingFile.PreventUploadComplete = false;
                synchronizingFile.Dispose();
                sourceMetadata["Content-MD5"] = synchronizingFile.FileHash;

                Storage.Batch(accessor => accessor.UpdateFileMetadata(tempFileName, sourceMetadata));

                Storage.Batch(accessor =>
                {
                    StorageOperationsTask.IndicateFileToDelete(fileName);
                    accessor.RenameFile(tempFileName, fileName);

                    Search.Delete(tempFileName);
                    Search.Index(fileName, sourceMetadata);
                });

                if (isNewFile)
                {
                    Log.Debug("Temporary downloading file '{0}' was renamed to '{1}'. Indexes was updated.", tempFileName, fileName);
                }
                else
                {
                    Log.Debug("Old file '{0}' was deleted. Indexes was updated.", fileName);
                }

                if (isConflictResolved)
                {
                    ConflictArtifactManager.Delete(fileName);
                    Publisher.Publish(new ConflictResolved { FileName = fileName });
                }
			}
			catch (Exception ex)
			{
				report.Exception = ex;
			}
			finally
			{
				if (localFile != null)
				{
					localFile.Dispose();
				}
			}

			if (report.Exception == null)
			{
				Log.Debug(
					"File '{0}' was synchronized successfully from {1}. {2} bytes were transfered and {3} bytes copied. Need list length was {4}",
					fileName, sourceServerInfo, report.BytesTransfered, report.BytesCopied, report.NeedListLength);
			}
			else
			{
				Log.WarnException(
					string.Format("Error has occurred during synchronization of a file '{0}' from {1}", fileName, sourceServerInfo),
					report.Exception);
			}

			FinishSynchronization(fileName, report, sourceServerInfo, sourceFileETag);

			PublishFileNotification(fileName, isNewFile ? FileChangeAction.Add : FileChangeAction.Update);
			PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Finish);

            return this.GetMessageWithObject(report, HttpStatusCode.OK);
		}

		private void FinishSynchronization(string fileName, SynchronizationReport report, ServerInfo sourceServer, Guid sourceFileETag)
		{
			try
			{
				// we want to execute those operation in a single batch but we also have to ensure that
				// Raven/Synchronization/Sources/sourceServerId config is modified only by one finishing synchronization at the same time
				SynchronizationFinishLocks.GetOrAdd(sourceServer.Id, new ReaderWriterLockSlim()).EnterWriteLock();

				Storage.Batch(accessor =>
				{
					SaveSynchronizationReport(fileName, accessor, report);
					FileLockManager.UnlockByDeletingSyncConfiguration(fileName, accessor);

					if (report.Exception == null)
					{
						SaveSynchronizationSourceInformation(sourceServer, sourceFileETag, accessor);
					}
				});
			}
			catch (Exception ex)
			{
				Log.ErrorException(
					string.Format("Failed to finish synchronization of a file '{0}' from {1}", fileName, sourceServer), ex);
			}
			finally
			{
				SynchronizationFinishLocks.GetOrAdd(sourceServer.Id, new ReaderWriterLockSlim()).ExitWriteLock();
			}
		}

        private void AssertConflictDetection(string fileName, RavenJObject localMetadata, RavenJObject sourceMetadata, ServerInfo sourceServer, out bool isConflictResolved)
		{
			var conflict = ConflictDetector.Check(fileName, localMetadata, sourceMetadata, sourceServer.FileSystemUrl);
			isConflictResolved = ConflictResolver.IsResolved(localMetadata, conflict);

			if (conflict != null && !isConflictResolved)
			{
				ConflictArtifactManager.Create(fileName, conflict);

				Publisher.Publish(new ConflictDetected
				{
					FileName = fileName,
					SourceServerUrl = sourceServer.FileSystemUrl
				});

				Log.Debug(
					"File '{0}' is in conflict with synchronized version from {1} ({2}). File marked as conflicted, conflict configuration item created",
					fileName, sourceServer.FileSystemUrl, sourceServer.Id);

				throw new SynchronizationException(string.Format("File {0} is conflicted", fileName));
			}
		}

		private void StartupProceed(string fileName, IStorageActionsAccessor accessor)
		{
			// remove previous SyncResult
			DeleteSynchronizationReport(fileName, accessor);

			// remove previous .downloading file
			StorageOperationsTask.IndicateFileToDelete(RavenFileNameHelper.DownloadingFileName(fileName));
		}

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/synchronization/UpdateMetadata/{*fileName}")]
		public HttpResponseMessage UpdateMetadata(string fileName)
		{
			var sourceServerInfo = InnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            // REVIEW: (Oren) It works, but it seems to me it is not an scalable solution. 
            var sourceFileETag = Guid.Parse(InnerHeaders.GetValues("ETag").First().Trim('\"'));

            Log.Debug("Starting to update a metadata of file '{0}' with ETag {1} from {2} because of synchronization", fileName,
					  sourceFileETag, sourceServerInfo);

			var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.MetadataUpdate);

			try
			{
				Storage.Batch(accessor =>
				{
					AssertFileIsNotBeingSynced(fileName, accessor);
					FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceServerInfo, accessor);
				});

				PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Start);

				Storage.Batch(accessor => StartupProceed(fileName, accessor));

				var localMetadata = GetLocalMetadata(fileName);
                var sourceMetadata = GetFilteredMetadataFromHeaders(InnerHeaders);

                bool isConflictResolved;

                AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);

                Historian.UpdateLastModified(sourceMetadata);

                Storage.Batch(accessor => accessor.UpdateFileMetadata(fileName, sourceMetadata));

                Search.Index(fileName, sourceMetadata);

                if (isConflictResolved)
                {
                    ConflictArtifactManager.Delete(fileName);
                    Publisher.Publish(new ConflictResolved { FileName = fileName });
                }

                PublishFileNotification(fileName, FileChangeAction.Update);
			}
			catch (Exception ex)
			{
				report.Exception = ex;

				Log.WarnException(
					string.Format("Error was occured during metadata synchronization of file '{0}' from {1}", fileName,
								  sourceServerInfo), ex);
			}
			finally
			{
				FinishSynchronization(fileName, report, sourceServerInfo, sourceFileETag);

				PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Finish);
			}

			if (report.Exception == null)
			{
				Log.Debug("Metadata of file '{0}' was synchronized successfully from {1}", fileName, sourceServerInfo);
			}

            return this.GetMessageWithObject(report, HttpStatusCode.OK);
		}


		[HttpDelete]
        [Route("ravenfs/{fileSystemName}/synchronization")]
		public HttpResponseMessage Delete(string fileName)
		{
			var sourceServerInfo = InnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            var sourceFileETag = Guid.Parse(InnerHeaders.GetValues("ETag").First().Trim('\"'));

            Log.Debug("Starting to delete a file '{0}' with ETag {1} from {2} because of synchronization", fileName, sourceFileETag, sourceServerInfo);

			var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.Delete);

			try
			{
				Storage.Batch(accessor =>
				{
					AssertFileIsNotBeingSynced(fileName, accessor);
					FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceServerInfo, accessor);
				});

				PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Start);

				Storage.Batch(accessor => StartupProceed(fileName, accessor));

				var localMetadata = GetLocalMetadata(fileName);

				if (localMetadata != null)
				{
                    // REVIEW: Use InnerHeaders for consistency?
                    var sourceMetadata = GetFilteredMetadataFromHeaders(Request.Headers); // Request.Headers.FilterHeadersToObject();

                    bool isConflictResolved;

                    AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);

                    Storage.Batch(accessor =>
                    {
                        StorageOperationsTask.IndicateFileToDelete(fileName);

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

                        Historian.UpdateLastModified(tombstoneMetadata);
                        accessor.PutFile(fileName, 0, tombstoneMetadata, true);
                    });

                    PublishFileNotification(fileName, FileChangeAction.Delete);
				}
			}
			catch (Exception ex)
			{
				report.Exception = ex;

				Log.WarnException(string.Format("Error was occurred during deletion synchronization of file '{0}' from {1}", fileName, sourceServerInfo), ex);
			}
			finally
			{
				FinishSynchronization(fileName, report, sourceServerInfo, sourceFileETag);

				PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Finish);
			}

			if (report.Exception == null)
			{
				Log.Debug("File '{0}' was deleted during synchronization from {1}", fileName, sourceServerInfo);
			}

            return this.GetMessageWithObject(report, HttpStatusCode.OK);
		}

		[HttpPatch]
        [Route("ravenfs/{fileSystemName}/synchronization/Rename")]
		public HttpResponseMessage Rename(string fileName, string rename)
		{
			var sourceServerInfo = InnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            var sourceFileETag = Guid.Parse(InnerHeaders.GetValues("ETag").First().Trim('\"'));
            var sourceMetadata = GetFilteredMetadataFromHeaders(InnerHeaders);

			Log.Debug("Starting to rename a file '{0}' to '{1}' with ETag {2} from {3} because of synchronization", fileName,
					  rename, sourceFileETag, sourceServerInfo);

			var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.Rename);

			try
			{
				Storage.Batch(accessor =>
				{
					AssertFileIsNotBeingSynced(fileName, accessor);
					FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceServerInfo, accessor);
				});

				PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Start);

				Storage.Batch(accessor => StartupProceed(fileName, accessor));

				var localMetadata = GetLocalMetadata(fileName);

				bool isConflictResolved;

                AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);

                StorageOperationsTask.RenameFile(new RenameFileOperation
                {
                    Name = fileName,
                    Rename = rename,
                    MetadataAfterOperation = sourceMetadata.WithETag(sourceFileETag).DropRenameMarkers()
                });
			}
			catch (Exception ex)
			{
				report.Exception = ex;
				Log.WarnException( string.Format("Error was occurred during renaming synchronization of file '{0}' from {1}", fileName, sourceServerInfo), ex);
			}
			finally
			{
				FinishSynchronization(fileName, report, sourceServerInfo, sourceFileETag);

				PublishSynchronizationNotification(fileName, sourceServerInfo, report.Type, SynchronizationAction.Finish);
			}

			if (report.Exception == null)
				Log.Debug("File '{0}' was renamed to '{1}' during synchronization from {2}", fileName, rename, sourceServerInfo);

            return this.GetMessageWithObject(report, HttpStatusCode.OK);
		}

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/synchronization/Confirm")]
		public async Task<IEnumerable<SynchronizationConfirmation>> Confirm()
		{
			var contentStream = await Request.Content.ReadAsStreamAsync();

			var confirmingFiles =
				new JsonSerializer().Deserialize<IEnumerable<Tuple<string, Guid>>>(
					new JsonTextReader(new StreamReader(contentStream)));

			return confirmingFiles.Select(file => new SynchronizationConfirmation
			{
				FileName = file.Item1,
				Status = CheckSynchronizedFileStatus(file)
			});
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/synchronization/Status")]
        public HttpResponseMessage Status(string fileName)
		{
			return Request.CreateResponse(HttpStatusCode.OK, GetSynchronizationReport(fileName));
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/synchronization/Finished")]
		public HttpResponseMessage Finished()
		{
			ListPage<SynchronizationReport> page = null;

			Storage.Batch(accessor =>
			{
				var configs = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncResultNamePrefix,
																 Paging.PageSize * Paging.Start, Paging.PageSize);
				var reports = configs.Select(config => config.JsonDeserialization<SynchronizationReport>()).ToList();
				page = new ListPage<SynchronizationReport>(reports, reports.Count);
			});

            return this.GetMessageWithObject(page, HttpStatusCode.OK);
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/synchronization/Active")]
		public HttpResponseMessage Active()
		{
            var result = new ListPage<SynchronizationDetails>(SynchronizationTask.Queue.Active
                                                                                       .Skip(Paging.PageSize * Paging.Start)
                                                                                       .Take(Paging.PageSize), 
                                                              SynchronizationTask.Queue.GetTotalActiveTasks());

            return this.GetMessageWithObject(result, HttpStatusCode.OK);
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/synchronization/Pending")]
		public HttpResponseMessage Pending()
		{
            var result = new ListPage<SynchronizationDetails>(SynchronizationTask.Queue.Pending
                                                                                       .Skip(Paging.PageSize * Paging.Start)
                                                                                       .Take(Paging.PageSize),
											                  SynchronizationTask.Queue.GetTotalPendingTasks());

            return this.GetMessageWithObject(result, HttpStatusCode.OK);
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/synchronization/Conflicts")]
		public HttpResponseMessage Conflicts()
		{
			ListPage<ConflictItem> page = null;

			Storage.Batch(accessor =>
			{
                var conflicts = accessor.GetConfigurationValuesStartWithPrefix<ConflictItem>(
                                                    RavenFileNameHelper.ConflictConfigNamePrefix,
													Paging.PageSize * Paging.Start,
													Paging.PageSize).ToList();

				page = new ListPage<ConflictItem>(conflicts, conflicts.Count);
			});

            return this.GetMessageWithObject(page, HttpStatusCode.OK);			
		}

		[HttpPatch]
        [Route("ravenfs/{fileSystemName}/synchronization/ResolveConflict/{*fileName}")]
		public HttpResponseMessage ResolveConflict(string fileName, ConflictResolutionStrategy strategy)
		{
			Log.Debug("Resolving conflict of a file '{0}' by using {1} strategy", fileName, strategy);

			if (strategy == ConflictResolutionStrategy.CurrentVersion)
			{
				StrategyAsGetCurrent(fileName);
			}
			else
			{
				StrategyAsGetRemote(fileName);
			}

            return GetEmptyMessage(HttpStatusCode.OK);
		}

		[HttpPatch]
        [Route("ravenfs/{fileSystemName}/synchronization/applyConflict/{*fileName}")]
		public async Task<HttpResponseMessage> ApplyConflict(string filename, long remoteVersion, string remoteServerId, string remoteServerUrl)
		{
			var localMetadata = GetLocalMetadata(filename);

			if (localMetadata == null)
				throw new HttpResponseException(HttpStatusCode.NotFound);

			var contentStream = await Request.Content.ReadAsStreamAsync();

			var current = new HistoryItem
			{
				ServerId = Storage.Id.ToString(),
				Version = localMetadata.Value<long>(SynchronizationConstants.RavenSynchronizationVersion)
			};

			var currentConflictHistory = Historian.DeserializeHistory(localMetadata);
			currentConflictHistory.Add(current);

			var remote = new HistoryItem
			{
				ServerId = remoteServerId,
				Version = remoteVersion
			};

			var remoteConflictHistory =
				new JsonSerializer().Deserialize<IList<HistoryItem>>(new JsonTextReader(new StreamReader(contentStream)));
			remoteConflictHistory.Add(remote);

			var conflict = new ConflictItem
			{
				CurrentHistory = currentConflictHistory,
				RemoteHistory = remoteConflictHistory,
				FileName = filename,
				RemoteServerUrl = Uri.UnescapeDataString(remoteServerUrl)
			};

			ConflictArtifactManager.Create(filename, conflict);

			Publisher.Publish(new ConflictDetected
			{
				FileName = filename,
				SourceServerUrl = remoteServerUrl
			});

			Log.Debug("Conflict applied for a file '{0}' (remote version: {1}, remote server id: {2}).", filename, remoteVersion, remoteServerId);

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/synchronization/LastSynchronization")]
		public HttpResponseMessage LastSynchronization(Guid from)
		{
			SourceSynchronizationInformation lastEtag = null;
			Storage.Batch(accessor => lastEtag = GetLastSynchronization(from, accessor));

			Log.Debug("Got synchronization last ETag request from {0}: [{1}]", from, lastEtag);

            return this.GetMessageWithObject(lastEtag, HttpStatusCode.OK);
		}

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/synchronization/IncrementLastETag")]
		public HttpResponseMessage IncrementLastETag(Guid sourceServerId, string sourceFileSystemUrl, Guid sourceFileETag)
		{
			try
			{
				// we want to execute those operation in a single batch but we also have to ensure that
				// Raven/Synchronization/Sources/sourceServerId config is modified only by one finishing synchronization at the same time
				SynchronizationFinishLocks.GetOrAdd(sourceServerId, new ReaderWriterLockSlim()).EnterWriteLock();

				Storage.Batch(
					accessor =>
					SaveSynchronizationSourceInformation(new ServerInfo { Id = sourceServerId, FileSystemUrl = sourceFileSystemUrl }, sourceFileETag,
														 accessor));
			}
			catch (Exception ex)
			{
				Log.ErrorException(
					string.Format("Failed to update last seen ETag from {0}", sourceServerId), ex);
			}
			finally
			{
				SynchronizationFinishLocks.GetOrAdd(sourceServerId, new ReaderWriterLockSlim()).ExitWriteLock();
			}

            return GetEmptyMessage(HttpStatusCode.OK);
		}

		private void PublishFileNotification(string fileName, FileChangeAction action)
		{
			Publisher.Publish(new FileChange
			{
				File = FilePathTools.Cannoicalise(fileName),
				Action = action
			});
		}

		private void PublishSynchronizationNotification(string fileName, ServerInfo sourceServer, SynchronizationType type, SynchronizationAction action)
		{
			Publisher.Publish(new SynchronizationUpdate
			{
				FileName = fileName,
				SourceFileSystemUrl = sourceServer.FileSystemUrl,
				SourceServerId = sourceServer.Id,
				Type = type,
				Action = action,
				SynchronizationDirection = SynchronizationDirection.Incoming
			});
		}

		private void StrategyAsGetCurrent(string fileName)
		{
			Storage.Batch(accessor =>
			{
                var conflict = accessor.GetConfigurationValue<ConflictItem>(RavenFileNameHelper.ConflictConfigNameForFile(fileName));

                var localMetadata = accessor.GetFile(fileName, 0, 0).Metadata;
                var localHistory = Historian.DeserializeHistory(localMetadata);

                // incorporate remote version history into local
                foreach (var remoteHistoryItem in conflict.RemoteHistory.Where(remoteHistoryItem => !localHistory.Contains(remoteHistoryItem)))
                {
                    localHistory.Add(remoteHistoryItem);
                }

                localMetadata[SynchronizationConstants.RavenSynchronizationHistory] = Historian.SerializeHistory(localHistory);

                accessor.UpdateFileMetadata(fileName, localMetadata);

                ConflictArtifactManager.Delete(fileName, accessor);
                Publisher.Publish(new ConflictResolved { FileName = fileName });
			});
		}

		private void StrategyAsGetRemote(string fileName)
		{
			Storage.Batch(
				accessor =>
				{
					var localMetadata = accessor.GetFile(fileName, 0, 0).Metadata;
					var conflictConfigName = RavenFileNameHelper.ConflictConfigNameForFile(fileName);
                    var conflictItem = accessor.GetConfig(conflictConfigName).JsonDeserialization<ConflictItem>();

					var conflictResolution = new ConflictResolution
						                        {
							                        Strategy = ConflictResolutionStrategy.RemoteVersion,
							                        RemoteServerId = conflictItem.RemoteHistory.Last().ServerId,
							                        Version = conflictItem.RemoteHistory.Last().Version,
						                        };

					localMetadata[SynchronizationConstants.RavenSynchronizationConflictResolution] = JsonExtensions.ToJObject(conflictResolution);
					accessor.UpdateFileMetadata(fileName, localMetadata);
				});
		}

		private FileStatus CheckSynchronizedFileStatus(Tuple<string, Guid> fileInfo)
		{
			var report = GetSynchronizationReport(fileInfo.Item1);

			if (report == null || report.FileETag != fileInfo.Item2)
				return FileStatus.Unknown;

			return report.Exception == null ? FileStatus.Safe : FileStatus.Broken;
		}

		private void SaveSynchronizationReport(string fileName, IStorageActionsAccessor accessor, SynchronizationReport report)
		{
			var name = RavenFileNameHelper.SyncResultNameForFile(fileName);
			accessor.SetConfig(name, JsonExtensions.ToJObject(report));
		}

		private void DeleteSynchronizationReport(string fileName, IStorageActionsAccessor accessor)
		{
			var name = RavenFileNameHelper.SyncResultNameForFile(fileName);
			accessor.DeleteConfig(name);
			Search.Delete(name);
		}

		private SynchronizationReport GetSynchronizationReport(string fileName)
		{
			SynchronizationReport preResult = null;

			Storage.Batch(
				accessor =>
				{
					try
					{
						var name = RavenFileNameHelper.SyncResultNameForFile(fileName);
                        preResult = accessor.GetConfig(name).JsonDeserialization<SynchronizationReport>();
					}
					catch (FileNotFoundException)
					{
						// just ignore
					}
				});

			return preResult;
		}

        private RavenJObject GetLocalMetadata(string fileName)
		{
            RavenJObject result = null;
            try
            {
                Storage.Batch(accessor => { result = accessor.GetFile(fileName, 0, 0).Metadata; });
            }
            catch (FileNotFoundException)
            {
                return null;
            }

            if (result.ContainsKey(SynchronizationConstants.RavenDeleteMarker))
            {
                return null;
            }

            return result;
		}

		private SourceSynchronizationInformation GetLastSynchronization(Guid from, IStorageActionsAccessor accessor)
		{
			SourceSynchronizationInformation info;
			try
			{
				info = accessor.GetConfig(SynchronizationConstants.RavenSynchronizationSourcesBasePath + "/" + from)
                               .JsonDeserialization<SourceSynchronizationInformation>();
			}
			catch (FileNotFoundException)
			{
				info = new SourceSynchronizationInformation
				{
					LastSourceFileEtag = Guid.Empty,
					DestinationServerId = Storage.Id
				};
			}

			return info;
		}

		private void SaveSynchronizationSourceInformation(ServerInfo sourceServer, Guid lastSourceEtag, IStorageActionsAccessor accessor)
		{
			var lastSynchronizationInformation = GetLastSynchronization(sourceServer.Id, accessor);
			if (Buffers.Compare(lastSynchronizationInformation.LastSourceFileEtag.ToByteArray(), lastSourceEtag.ToByteArray()) > 0)
			{
				return;
			}

			var synchronizationSourceInfo = new SourceSynchronizationInformation
			{
				LastSourceFileEtag = lastSourceEtag,
				SourceServerUrl = sourceServer.FileSystemUrl,
				DestinationServerId = Storage.Id
			};

			var key = SynchronizationConstants.RavenSynchronizationSourcesBasePath + "/" + sourceServer.Id;

			accessor.SetConfig(key, JsonExtensions.ToJObject(synchronizationSourceInfo));

			Log.Debug("Saved last synchronized file ETag {0} from {1} ({2})", lastSourceEtag, sourceServer.FileSystemUrl, sourceServer.Id);
		}
	}
}