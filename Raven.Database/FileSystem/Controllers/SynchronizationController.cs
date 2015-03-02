using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Multipart;
using Raven.Database.FileSystem.Util;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Data;

namespace Raven.Database.FileSystem.Controllers
{
	public class SynchronizationController : RavenFsApiController
	{
		private static new readonly ILog Log = LogManager.GetCurrentClassLogger();

		private static readonly ConcurrentDictionary<Guid, ReaderWriterLockSlim> SynchronizationFinishLocks =
			new ConcurrentDictionary<Guid, ReaderWriterLockSlim>();

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/ToDestinations")]
        public async Task<HttpResponseMessage> ToDestinations(bool forceSyncingAll)
        {
            var result = await SynchronizationTask.SynchronizeDestinationsAsync(forceSyncingAll);

            return this.GetMessageWithObject(result, HttpStatusCode.OK);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/ToDestination")]
        public async Task<HttpResponseMessage> ToDestination(string destination, bool forceSyncingAll)
        {
            var result = await SynchronizationTask.SynchronizeDestinationAsync(destination + "/fs/" + this.FileSystemName, forceSyncingAll);
            
            return this.GetMessageWithObject(result, HttpStatusCode.OK);
        }

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/start/{*filename}")]
        public async Task<HttpResponseMessage> Start(string filename)
		{
            var canonicalFilename = FileHeader.Canonize(filename);

		    var destination = await ReadJsonObjectAsync<SynchronizationDestination>();

            Log.Debug("Starting to synchronize a file '{0}' to {1}", canonicalFilename, destination.Url);

            var result = await SynchronizationTask.SynchronizeFileToAsync(canonicalFilename, destination);

            return this.GetMessageWithObject(result, HttpStatusCode.OK);
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/MultipartProceed")]
        public async Task<HttpResponseMessage> MultipartProceed(string fileSystemName)
		{
			if (!Request.Content.IsMimeMultipartContent())
				throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);

            var fileName = Request.Headers.GetValues(SyncingMultipartConstants.FileName).FirstOrDefault();
            var canonicalFilename = FileHeader.Canonize(fileName);

            var tempFileName = RavenFileNameHelper.DownloadingFileName(canonicalFilename);

            var sourceServerInfo = ReadInnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            var sourceFileETag = Guid.Parse(GetHeader(Constants.MetadataEtagField).Trim('\"'));

            var report = new SynchronizationReport(canonicalFilename, sourceFileETag, SynchronizationType.ContentUpdate);

			Log.Debug("Starting to process multipart synchronization request of a file '{0}' with ETag {1} from {2}", fileName, sourceFileETag, sourceServerInfo);

			StorageStream localFile = null;
			var isNewFile = false;
			var isConflictResolved = false;

			try
			{
				Storage.Batch(accessor =>
				{
                    AssertFileIsNotBeingSynced(canonicalFilename, accessor);
                    FileLockManager.LockByCreatingSyncConfiguration(canonicalFilename, sourceServerInfo, accessor);
				});

                SynchronizationTask.IncomingSynchronizationStarted(canonicalFilename, sourceServerInfo, sourceFileETag, SynchronizationType.ContentUpdate);

                PublishSynchronizationNotification(fileSystemName, canonicalFilename, sourceServerInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(canonicalFilename, accessor));

                RavenJObject sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

                var localMetadata = GetLocalMetadata(canonicalFilename);
                if (localMetadata != null)
                {
                    AssertConflictDetection(canonicalFilename, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);
                    localFile = StorageStream.Reading(Storage, canonicalFilename);
                }
                else
                {
                    isNewFile = true;
                }

                Historian.UpdateLastModified(sourceMetadata);
                
                var synchronizingFile = SynchronizingFileStream.CreatingOrOpeningAndWriting(Storage, Search, StorageOperationsTask, tempFileName, sourceMetadata);

                var provider = new MultipartSyncStreamProvider(synchronizingFile, localFile);

                Log.Debug("Starting to process/read multipart content of a file '{0}'", fileName);

                await Request.Content.ReadAsMultipartAsync(provider);

                Log.Debug("Multipart content of a file '{0}' was processed/read", fileName);

                report.BytesCopied = provider.BytesCopied;
                report.BytesTransfered = provider.BytesTransfered;
                report.NeedListLength = provider.NumberOfFileParts;

                synchronizingFile.PreventUploadComplete = false;
                synchronizingFile.Flush();
                synchronizingFile.Dispose();
                sourceMetadata["Content-MD5"] = synchronizingFile.FileHash;

                Storage.Batch(accessor => accessor.UpdateFileMetadata(tempFileName, sourceMetadata));

                Storage.Batch(accessor =>
                {
                    StorageOperationsTask.IndicateFileToDelete(canonicalFilename);
                    accessor.RenameFile(tempFileName, canonicalFilename);

                    Search.Delete(tempFileName);
                    Search.Index(canonicalFilename, sourceMetadata);
                });                

                if (isNewFile)
                {
                    Log.Debug("Temporary downloading file '{0}' was renamed to '{1}'. Indexes were updated.", tempFileName, fileName);
                }
                else
                {
                    Log.Debug("Old file '{0}' was deleted. Indexes were updated.", fileName);
                }

                if (isConflictResolved)
                {
                    ConflictArtifactManager.Delete(canonicalFilename);
                }    
        
			}
			catch (Exception ex)
			{
				if (ShouldAddExceptionToReport(ex))
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

            FinishSynchronization(canonicalFilename, report, sourceServerInfo, sourceFileETag);

			PublishFileNotification(fileName, isNewFile ? FileChangeAction.Add : FileChangeAction.Update);
            PublishSynchronizationNotification(fileSystemName, fileName, sourceServerInfo, report.Type, SynchronizationAction.Finish);

            if (isConflictResolved)
            {
                Publisher.Publish(new ConflictNotification
                {
                    FileName = fileName,
                    Status = ConflictStatus.Resolved
                });
            }    

            return GetMessageWithObject(report);
		}

		private void FinishSynchronization(string fileName, SynchronizationReport report, ServerInfo sourceServer, Guid sourceFileETag)
		{
			try
			{
				// we want to execute those operation in a single batch but we also have to ensure that
				// Raven/Synchronization/Sources/sourceServerId config is modified only by one finishing synchronization at the same time
				SynchronizationFinishLocks.GetOrAdd(sourceServer.Id, new ReaderWriterLockSlim()).EnterWriteLock();
                SynchronizationTask.IncomingSynchronizationFinished(fileName, sourceServer, sourceFileETag);

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
	        if (conflict == null)
	        {
		        isConflictResolved = false;
		        return;
	        }

			isConflictResolved = ConflictResolver.CheckIfResolvedByRemoteStrategy(localMetadata, conflict);

			if(isConflictResolved)
				return;

			ConflictResolutionStrategy strategy;
			if (ConflictResolver.TryResolveConflict(fileName, conflict, localMetadata, sourceMetadata, out strategy))
			{
				switch (strategy)
				{
					case ConflictResolutionStrategy.RemoteVersion:
						Log.Debug("Conflict automatically resolved by choosing remote version of the file {0}", fileName);
						return;
					case ConflictResolutionStrategy.CurrentVersion:

						Storage.Batch(accessor =>
						{
							accessor.UpdateFileMetadata(fileName, localMetadata);

							ConflictArtifactManager.Delete(fileName, accessor);
						});

						Log.Debug("Conflict automatically resolved by choosing current version of the file {0}", fileName);

						throw new ConflictResolvedInFavourOfCurrentVersionException();
				}
			}

			ConflictArtifactManager.Create(fileName, conflict);

			Publisher.Publish(new ConflictNotification
			{
				FileName = fileName,
				SourceServerUrl = sourceServer.FileSystemUrl,
				Status = ConflictStatus.Detected,
				RemoteFileHeader = new FileHeader(fileName, localMetadata)
			});

			Log.Debug(
				"File '{0}' is in conflict with synchronized version from {1} ({2}). File marked as conflicted, conflict configuration item created",
				fileName, sourceServer.FileSystemUrl, sourceServer.Id);

			throw new SynchronizationException(string.Format("File {0} is conflicted", fileName));
		}

		private void StartupProceed(string fileName, IStorageActionsAccessor accessor)
		{
			// remove previous SyncResult
			DeleteSynchronizationReport(fileName, accessor);

			// remove previous .downloading file
			StorageOperationsTask.IndicateFileToDelete(RavenFileNameHelper.DownloadingFileName(fileName));
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/UpdateMetadata/{*fileName}")]
        public HttpResponseMessage UpdateMetadata(string fileSystemName, string fileName)
		{
            bool isConflictResolved = false;

            var canonicalFilename = FileHeader.Canonize(fileName);

            var sourceServerInfo = ReadInnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            // REVIEW: (Oren) It works, but it seems to me it is not an scalable solution. 
            var sourceFileETag = Guid.Parse(GetHeader(Constants.MetadataEtagField).Trim('\"'));

            Log.Debug("Starting to update a metadata of file '{0}' with ETag {1} from {2} because of synchronization", fileName,
					  sourceFileETag, sourceServerInfo);

            var report = new SynchronizationReport(canonicalFilename, sourceFileETag, SynchronizationType.MetadataUpdate);

			try
			{
				Storage.Batch(accessor =>
				{
                    AssertFileIsNotBeingSynced(canonicalFilename, accessor);
                    FileLockManager.LockByCreatingSyncConfiguration(canonicalFilename, sourceServerInfo, accessor);
				});

                SynchronizationTask.IncomingSynchronizationStarted(canonicalFilename, sourceServerInfo, sourceFileETag, SynchronizationType.MetadataUpdate);

                PublishSynchronizationNotification(fileSystemName, canonicalFilename, sourceServerInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(canonicalFilename, accessor));

                var localMetadata = GetLocalMetadata(canonicalFilename);
                var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

                AssertConflictDetection(canonicalFilename, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);

                Historian.UpdateLastModified(sourceMetadata);

                Storage.Batch(accessor => accessor.UpdateFileMetadata(canonicalFilename, sourceMetadata));

                Search.Index(canonicalFilename, sourceMetadata);

                if (isConflictResolved)
                {
                    ConflictArtifactManager.Delete(canonicalFilename);
                }

                PublishFileNotification(fileName, FileChangeAction.Update);
			}
			catch (Exception ex)
			{
				if (ShouldAddExceptionToReport(ex))
				{
					report.Exception = ex;

					Log.WarnException(
						string.Format("Error was occurred during metadata synchronization of file '{0}' from {1}", fileName, sourceServerInfo), ex);
				}
			}
			finally
			{
                FinishSynchronization(canonicalFilename, report, sourceServerInfo, sourceFileETag);                
			}

            PublishSynchronizationNotification(fileSystemName, fileName, sourceServerInfo, report.Type, SynchronizationAction.Finish);

            if (isConflictResolved )
            {
                Publisher.Publish(new ConflictNotification
                {
                    FileName = fileName,
                    Status = ConflictStatus.Resolved
                });
            }

			if (report.Exception == null)
			{
				Log.Debug("Metadata of file '{0}' was synchronized successfully from {1}", fileName, sourceServerInfo);
			}

            return this.GetMessageWithObject(report, HttpStatusCode.OK);
		}


		[HttpDelete]
        [RavenRoute("fs/{fileSystemName}/synchronization")]
        public HttpResponseMessage Delete(string fileSystemName, string fileName)
		{
            var canonicalFilename = FileHeader.Canonize(fileName);

            var sourceServerInfo = ReadInnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            var sourceFileETag = Guid.Parse(GetHeader(Constants.MetadataEtagField).Trim('\"'));

            Log.Debug("Starting to delete a file '{0}' with ETag {1} from {2} because of synchronization", fileName, sourceFileETag, sourceServerInfo);

            var report = new SynchronizationReport(canonicalFilename, sourceFileETag, SynchronizationType.Delete);

			try
			{
				Storage.Batch(accessor =>
				{
                    AssertFileIsNotBeingSynced(canonicalFilename, accessor);
                    FileLockManager.LockByCreatingSyncConfiguration(canonicalFilename, sourceServerInfo, accessor);
				});


                SynchronizationTask.IncomingSynchronizationStarted(canonicalFilename, sourceServerInfo, sourceFileETag, SynchronizationType.Delete);

                PublishSynchronizationNotification(fileSystemName, canonicalFilename, sourceServerInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(canonicalFilename, accessor));

                var localMetadata = GetLocalMetadata(canonicalFilename);

				if (localMetadata != null)
				{
                    // REVIEW: Use InnerHeaders for consistency?
                    var sourceMetadata = GetFilteredMetadataFromHeaders(Request.Headers); // Request.Headers.FilterHeadersToObject();

                    bool isConflictResolved;

                    AssertConflictDetection(canonicalFilename, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);

                    Storage.Batch(accessor =>
                    {
                        StorageOperationsTask.IndicateFileToDelete(canonicalFilename);

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
                        accessor.PutFile(canonicalFilename, 0, tombstoneMetadata, true);
                    });

                    PublishFileNotification(fileName, FileChangeAction.Delete);
				}
			}
			catch (Exception ex)
			{
				if (ShouldAddExceptionToReport(ex))
				{
					report.Exception = ex;

					Log.WarnException(string.Format("Error was occurred during deletion synchronization of file '{0}' from {1}", fileName, sourceServerInfo), ex);
				}
			}
			finally
			{
                FinishSynchronization(canonicalFilename, report, sourceServerInfo, sourceFileETag);               
			}

            PublishSynchronizationNotification(fileSystemName, fileName, sourceServerInfo, report.Type, SynchronizationAction.Finish);

			if (report.Exception == null)
			{
				Log.Debug("File '{0}' was deleted during synchronization from {1}", fileName, sourceServerInfo);
			}

            return this.GetMessageWithObject(report, HttpStatusCode.OK);
		}

		[HttpPatch]
        [RavenRoute("fs/{fileSystemName}/synchronization/Rename")]
        public HttpResponseMessage Rename(string fileSystemName, string fileName, string rename)
		{
            bool isConflictResolved = false;

            var canonicalFilename = FileHeader.Canonize(fileName);
            var canonicalRename = FileHeader.Canonize(rename);

            var sourceServerInfo = ReadInnerHeaders.Value<ServerInfo>(SyncingMultipartConstants.SourceServerInfo);
            var sourceFileETag = Guid.Parse(GetHeader(Constants.MetadataEtagField).Trim('\"'));
            var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

			Log.Debug("Starting to rename a file '{0}' to '{1}' with ETag {2} from {3} because of synchronization", fileName,
					  rename, sourceFileETag, sourceServerInfo);

            var report = new SynchronizationReport(canonicalFilename, sourceFileETag, SynchronizationType.Rename);

			try
			{
				Storage.Batch(accessor =>
				{
                    AssertFileIsNotBeingSynced(canonicalFilename, accessor);
                    FileLockManager.LockByCreatingSyncConfiguration(canonicalFilename, sourceServerInfo, accessor);
				});

                SynchronizationTask.IncomingSynchronizationStarted(canonicalFilename, sourceServerInfo, sourceFileETag, SynchronizationType.Rename);

                PublishSynchronizationNotification(fileSystemName, canonicalFilename, sourceServerInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(canonicalFilename, accessor));

                var localMetadata = GetLocalMetadata(canonicalFilename);

                AssertConflictDetection(canonicalFilename, localMetadata, sourceMetadata, sourceServerInfo, out isConflictResolved);
                
				if (isConflictResolved)
                {
                    ConflictArtifactManager.Delete(canonicalFilename); 
                }
					
                StorageOperationsTask.RenameFile(new RenameFileOperation
                {
                    FileSystem = FileSystem.Name,
                    Name = canonicalFilename,
                    Rename = canonicalRename,
                    MetadataAfterOperation = sourceMetadata.WithETag(sourceFileETag).DropRenameMarkers()
                });
			}
			catch (Exception ex)
			{
				if (ShouldAddExceptionToReport(ex))
				{
					report.Exception = ex;
					Log.WarnException(string.Format("Error was occurred during renaming synchronization of file '{0}' from {1}", fileName, sourceServerInfo), ex);
				}
			}
			finally
			{
                FinishSynchronization(canonicalFilename, report, sourceServerInfo, sourceFileETag);               
			}

            PublishSynchronizationNotification(fileSystemName, canonicalFilename, sourceServerInfo, report.Type, SynchronizationAction.Finish);

            if (isConflictResolved)
            {
                Publisher.Publish(new ConflictNotification
                {
                    FileName = fileName,
                    Status = ConflictStatus.Resolved
                });
            }

			if (report.Exception == null)
				Log.Debug("File '{0}' was renamed to '{1}' during synchronization from {2}", fileName, rename, sourceServerInfo);

            return GetMessageWithObject(report);
		}

		private static bool ShouldAddExceptionToReport(Exception ex)
		{
			return ex is ConflictResolvedInFavourOfCurrentVersionException == false;
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/Confirm")]
        public async Task<HttpResponseMessage> Confirm()
		{
			var contentStream = await Request.Content.ReadAsStreamAsync();

			var confirmingFiles = JsonExtensions.CreateDefaultJsonSerializer()
				.Deserialize<IEnumerable<Tuple<string, Guid>>>(new JsonTextReader(new StreamReader(contentStream)));


            var result = confirmingFiles.Select(x =>
            {
                string canonicalFilename = FileHeader.Canonize(x.Item1);
                return new SynchronizationConfirmation 
                {
                    FileName = canonicalFilename,
                    Status = CheckSynchronizedFileStatus(canonicalFilename, x.Item2)
                };
            });         

            return this.GetMessageWithObject(result)
                       .WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/Status")]
        public HttpResponseMessage Status(string fileName)
		{
            fileName = FileHeader.Canonize(fileName);

            var report = GetSynchronizationReport(fileName);

            return this.GetMessageWithObject(report)
                       .WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/Finished")]
		public HttpResponseMessage Finished()
		{
			ItemsPage<SynchronizationReport> page = null;

			Storage.Batch(accessor =>
			{
				var configs = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncResultNamePrefix,
                                                                 Paging.Start, Paging.PageSize);
                int totalCount = 0;
                accessor.GetConfigNamesStartingWithPrefix(RavenFileNameHelper.SyncResultNamePrefix,
                                                                 Paging.Start, Paging.PageSize, out totalCount);

				var reports = configs.Select(config => config.JsonDeserialization<SynchronizationReport>()).ToList();
                page = new ItemsPage<SynchronizationReport>(reports, totalCount);
			});

            return this.GetMessageWithObject(page, HttpStatusCode.OK)
                       .WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/Active")]
		public HttpResponseMessage Active()
		{
            var result = new ItemsPage<SynchronizationDetails>(SynchronizationTask.Queue.Active
                                                                                       .Skip(Paging.Start)
                                                                                       .Take(Paging.PageSize), 
                                                              SynchronizationTask.Queue.GetTotalActiveTasks());

            return this.GetMessageWithObject(result, HttpStatusCode.OK)
                       .WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/Pending")]
		public HttpResponseMessage Pending()
		{
            var result = new ItemsPage<SynchronizationDetails>(SynchronizationTask.Queue.Pending
                                                                                       .Skip(Paging.Start)
                                                                                       .Take(Paging.PageSize),
											                  SynchronizationTask.Queue.GetTotalPendingTasks());

            return this.GetMessageWithObject(result, HttpStatusCode.OK)
                       .WithNoCache();
		}

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/Incoming")]
        public HttpResponseMessage Incoming()
        {
            var activeIncoming = SynchronizationTask.IncomingQueue;

            var result = new ItemsPage<SynchronizationDetails>(activeIncoming.Skip(Paging.Start)
                                                                            .Take(Paging.PageSize),
                                                              activeIncoming.Count());

            return this.GetMessageWithObject(result, HttpStatusCode.OK)
                       .WithNoCache();
        }


		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/Conflicts")]
		public HttpResponseMessage Conflicts()
		{
			ItemsPage<ConflictItem> page = null;

			Storage.Batch(accessor =>
			{
                var conflicts = accessor.GetConfigurationValuesStartWithPrefix<ConflictItem>(
                                                    RavenFileNameHelper.ConflictConfigNamePrefix,
													Paging.PageSize * Paging.Start,
													Paging.PageSize).ToList();

				page = new ItemsPage<ConflictItem>(conflicts, conflicts.Count);
			});

            return this.GetMessageWithObject(page, HttpStatusCode.OK)
                       .WithNoCache();		
		}

		[HttpPatch]
        [RavenRoute("fs/{fileSystemName}/synchronization/ResolveConflict/{*filename}")]
        public HttpResponseMessage ResolveConflict(string filename, ConflictResolutionStrategy strategy)
		{
            var canonicalFilename = FileHeader.Canonize(filename);

            Log.Debug("Resolving conflict of a file '{0}' by using {1} strategy", filename, strategy);

			switch (strategy)
			{
				case ConflictResolutionStrategy.CurrentVersion:

					Storage.Batch(accessor =>
					{
                        var localMetadata = accessor.GetFile(canonicalFilename, 0, 0).Metadata;
                        var conflict = accessor.GetConfigurationValue<ConflictItem>(RavenFileNameHelper.ConflictConfigNameForFile(canonicalFilename));

                        ConflictResolver.ApplyCurrentStrategy(canonicalFilename, conflict, localMetadata);

                        accessor.UpdateFileMetadata(canonicalFilename, localMetadata);

                        ConflictArtifactManager.Delete(canonicalFilename, accessor);
					});

					Publisher.Publish(new ConflictNotification
					{
                        FileName = filename,
						Status = ConflictStatus.Resolved
					});

					break;
				case ConflictResolutionStrategy.RemoteVersion:

					Storage.Batch(accessor =>
					{
                        var localMetadata = accessor.GetFile(canonicalFilename, 0, 0).Metadata;
                        var conflict = accessor.GetConfig(RavenFileNameHelper.ConflictConfigNameForFile(canonicalFilename)).JsonDeserialization<ConflictItem>();

                        ConflictResolver.ApplyRemoteStrategy(canonicalFilename, conflict, localMetadata);

                        accessor.UpdateFileMetadata(canonicalFilename, localMetadata);

                        // ConflictArtifactManager.Delete(canonicalFilename, accessor); - intentionally not deleting, conflict item will be removed when a remote file is put
					});

					Task.Run(() => SynchronizationTask.SynchronizeDestinationsAsync(true));
					break;
				default:
					throw new NotSupportedException(string.Format("{0} is not the valid strategy to resolve a conflict", strategy));
			}

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPost]
		[RavenRoute("fs/{fileSystemName}/synchronization/ResolutionStrategyFromServerResolvers")]
		public async Task<HttpResponseMessage> ResolutionStrategyFromServerResolvers()
		{
			var conflict = await ReadJsonObjectAsync<ConflictItem>();

			var localMetadata = GetLocalMetadata(conflict.FileName);
            if (localMetadata == null)
				throw new InvalidOperationException(string.Format("Could not find the medatada of the file: {0}", conflict.FileName));

			var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

			ConflictResolutionStrategy strategy;

			if (ConflictResolver.TryResolveConflict(conflict.FileName, conflict, localMetadata, sourceMetadata, out strategy))
			{
				return GetMessageWithObject(strategy);
			}

			return GetMessageWithObject(ConflictResolutionStrategy.NoResolution);
		}

		[HttpPatch]
        [RavenRoute("fs/{fileSystemName}/synchronization/applyConflict/{*fileName}")]
		public async Task<HttpResponseMessage> ApplyConflict(string filename, long remoteVersion, string remoteServerId, string remoteServerUrl)
		{
            var canonicalFilename = FileHeader.Canonize(filename);

            var localMetadata = GetLocalMetadata(canonicalFilename);

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

            var remoteMetadata = RavenJObject.Load(new JsonTextReader(new StreamReader(contentStream)));

            var remoteConflictHistory = Historian.DeserializeHistory(remoteMetadata);
			remoteConflictHistory.Add(remote);

			var conflict = new ConflictItem
			{
				CurrentHistory = currentConflictHistory,
				RemoteHistory = remoteConflictHistory,
                FileName = canonicalFilename,
				RemoteServerUrl = Uri.UnescapeDataString(remoteServerUrl)
			};

            ConflictArtifactManager.Create(canonicalFilename, conflict);

            Publisher.Publish(new ConflictNotification
			{
                FileName = filename,
				SourceServerUrl = remoteServerUrl,
                Status = ConflictStatus.Detected,
                RemoteFileHeader = new FileHeader(canonicalFilename, remoteMetadata)
			});

			Log.Debug("Conflict applied for a file '{0}' (remote version: {1}, remote server id: {2}).", filename, remoteVersion, remoteServerId);

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/LastSynchronization")]
		public HttpResponseMessage LastSynchronization(Guid from)
		{
			SourceSynchronizationInformation lastEtag = null;
			Storage.Batch(accessor => lastEtag = GetLastSynchronization(from, accessor));

			Log.Debug("Got synchronization last ETag request from {0}: [{1}]", from, lastEtag);

            return this.GetMessageWithObject(lastEtag, HttpStatusCode.OK)
                       .WithNoCache();
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/IncrementLastETag")]
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
			Publisher.Publish(new FileChangeNotification
			{
				File = FilePathTools.Cannoicalise(fileName),
				Action = action
			});
		}

		private void PublishSynchronizationNotification(string fileSystemName, string fileName, ServerInfo sourceServer, SynchronizationType type, SynchronizationAction action)
		{
			Publisher.Publish(new SynchronizationUpdateNotification
			{
				FileName = fileName,
				SourceFileSystemUrl = sourceServer.FileSystemUrl,
				SourceServerId = sourceServer.Id,
				Type = type,
				Action = action,
				Direction = SynchronizationDirection.Incoming
			});
		}

        private FileStatus CheckSynchronizedFileStatus(string filename, Guid etag)
		{
            var report = GetSynchronizationReport(filename);
            if (report == null || report.FileETag != etag)
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


	    protected override RavenJObject GetFilteredMetadataFromHeaders(IEnumerable<KeyValuePair<string, IEnumerable<string>>> headers)
	    {
	        string lastModifed = null;

			var result = base.GetFilteredMetadataFromHeaders(headers.Select(h =>
			{
			    if ( lastModifed == null && h.Key == Constants.RavenLastModified)
			    {
			        lastModifed = h.Value.First();
			    }
			    return h;
			}));

            if (lastModifed != null)
			{
				// this is required to resolve conflicts based on last modification date

                result.Add(Constants.RavenLastModified, lastModifed);
			}

			return result;
		}
	}
}