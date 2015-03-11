using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
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
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Database.FileSystem.Controllers
{
	public class SynchronizationController : RavenFsApiController
	{
		private static new readonly ILog Log = LogManager.GetCurrentClassLogger();

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/ToDestinations")]
        public async Task<HttpResponseMessage> ToDestinations(bool forceSyncingAll)
        {
            var result = await SynchronizationTask.Execute(forceSyncingAll);

            return GetMessageWithObject(result);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/ToDestination")]
        public async Task<HttpResponseMessage> ToDestination(string destination, bool forceSyncingAll)
        {
            var result = await SynchronizationTask.SynchronizeDestinationAsync(destination + "/fs/" + this.FileSystemName, forceSyncingAll);
            
            return GetMessageWithObject(result);
        }

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/start/{*filename}")]
        public async Task<HttpResponseMessage> Start(string filename)
		{
            var canonicalFilename = FileHeader.Canonize(filename);

		    var destination = await ReadJsonObjectAsync<SynchronizationDestination>();

            Log.Debug("Starting to synchronize a file '{0}' to {1}", canonicalFilename, destination.Url);

            var result = await SynchronizationTask.SynchronizeFileToAsync(canonicalFilename, destination);

            return GetMessageWithObject(result);
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/MultipartProceed")]
        public async Task<HttpResponseMessage> MultipartProceed()
		{
			if (!Request.Content.IsMimeMultipartContent())
				throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);

            var fileName = FileHeader.Canonize(Request.Headers.GetValues(SyncingMultipartConstants.FileName).FirstOrDefault());

            var tempFileName = RavenFileNameHelper.DownloadingFileName(fileName);

			var sourceInfo = GetSourceFileSystemInfo();
			var sourceFileETag = GetEtag();

            var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.ContentUpdate);

			Log.Debug("Starting to process multipart synchronization request of a file '{0}' with ETag {1} from {2}", fileName, sourceFileETag, sourceInfo);

			StorageStream localFile = null;
			var isConflictResolved = false;

			try
			{
				var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

				Storage.Batch(accessor =>
				{
					Files.AssertPutOperationNotVetoed(fileName, sourceMetadata);
                    Synchronizations.AssertFileIsNotBeingSynced(fileName);
                    FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceInfo, accessor);
				});

				FileSystem.PutTriggers.Apply(trigger => trigger.BeforeSynchronization(fileName, sourceMetadata));

                SynchronizationTask.IncomingSynchronizationStarted(fileName, sourceInfo, sourceFileETag, SynchronizationType.ContentUpdate);

                Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(fileName, accessor)); //TODO arek - move this logic to separate trigger and BeforeSynchronizationMethod

                var localMetadata = Synchronizations.GetLocalMetadata(fileName);
                if (localMetadata != null)
                {
                    AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceInfo, out isConflictResolved);
                    localFile = StorageStream.Reading(Storage, fileName);
                }

				FileSystem.PutTriggers.Apply(trigger => trigger.OnPut(tempFileName, sourceMetadata));

                Historian.UpdateLastModified(sourceMetadata);
                
                var synchronizingFile = SynchronizingFileStream.CreatingOrOpeningAndWriting(FileSystem, tempFileName, sourceMetadata);

				FileSystem.PutTriggers.Apply(trigger => trigger.AfterPut(tempFileName, null, sourceMetadata));

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

				MetadataUpdateResult updateResult = null;
                Storage.Batch(accessor => updateResult = accessor.UpdateFileMetadata(tempFileName, sourceMetadata, null));

                Storage.Batch(accessor =>
                {
	                using (FileSystem.DisableAllTriggersForCurrentThread())
	                {
		                Files.IndicateFileToDelete(fileName, null);
	                }

					FileSystem.PutTriggers.Apply(trigger => trigger.AfterSynchronization(fileName, tempFileName, sourceMetadata));

	                accessor.RenameFile(tempFileName, fileName);

                    Search.Delete(tempFileName);
                    Search.Index(fileName, sourceMetadata, updateResult.Etag);
                });                

				if (localFile == null)
                    Log.Debug("Temporary downloading file '{0}' was renamed to '{1}'. Indexes were updated.", tempFileName, fileName);
                else
                    Log.Debug("Old file '{0}' was deleted. Indexes were updated.", fileName);

                if (isConflictResolved)
					ConflictArtifactManager.Delete(fileName);
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
					fileName, sourceInfo, report.BytesTransfered, report.BytesCopied, report.NeedListLength);
			}
			else
			{
				Log.WarnException(
					string.Format("Error has occurred during synchronization of a file '{0}' from {1}", fileName, sourceInfo),
					report.Exception);
			}

			Synchronizations.FinishSynchronization(fileName, report, sourceInfo, sourceFileETag);

			Synchronizations.PublishFileNotification(fileName, localFile == null ? FileChangeAction.Add : FileChangeAction.Update);
            Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Finish);

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

        private void AssertConflictDetection(string fileName, RavenJObject localMetadata, RavenJObject sourceMetadata, FileSystemInfo sourceFileSystem, out bool isConflictResolved)
		{
			var conflict = ConflictDetector.Check(fileName, localMetadata, sourceMetadata, sourceFileSystem.Url);
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
							accessor.UpdateFileMetadata(fileName, localMetadata, null);

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
				SourceServerUrl = sourceFileSystem.Url,
				Status = ConflictStatus.Detected,
				RemoteFileHeader = new FileHeader(fileName, localMetadata)
			});

			Log.Debug(
				"File '{0}' is in conflict with synchronized version from {1} ({2}). File marked as conflicted, conflict configuration item created",
				fileName, sourceFileSystem.Url, sourceFileSystem.Id);

			throw new SynchronizationException(string.Format("File {0} is conflicted", fileName));
		}

		private void StartupProceed(string fileName, IStorageActionsAccessor accessor)
		{
			// remove previous SyncResult
			Synchronizations.DeleteSynchronizationReport(fileName, accessor);

			// remove previous .downloading file
			Files.IndicateFileToDelete(RavenFileNameHelper.DownloadingFileName(fileName), null);
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/UpdateMetadata/{*fileName}")]
        public HttpResponseMessage UpdateMetadata(string fileName)
		{
            bool isConflictResolved = false;

            fileName = FileHeader.Canonize(fileName);

			var sourceInfo = GetSourceFileSystemInfo();
			var sourceFileETag = GetEtag();
			var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

            Log.Debug("Starting to update a metadata of file '{0}' with ETag {1} from {2} because of synchronization", fileName,
					  sourceFileETag, sourceInfo);

            var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.MetadataUpdate);

			try
			{
				Storage.Batch(accessor =>
				{
                    Synchronizations.AssertFileIsNotBeingSynced(fileName);
					Files.AssertMetadataUpdateOperationNotVetoed(fileName, sourceMetadata);
                    FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceInfo, accessor);
				});

                SynchronizationTask.IncomingSynchronizationStarted(fileName, sourceInfo, sourceFileETag, SynchronizationType.MetadataUpdate);

                Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(fileName, accessor));

				var localMetadata = Synchronizations.GetLocalMetadata(fileName);
               
                AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceInfo, out isConflictResolved);

                Historian.UpdateLastModified(sourceMetadata);

				FileSystem.MetadataUpdateTriggers.Apply(trigger => trigger.OnUpdate(fileName, sourceMetadata));

				MetadataUpdateResult updateMetadata = null;
                Storage.Batch(accessor => updateMetadata = accessor.UpdateFileMetadata(fileName, sourceMetadata, null));

				FileSystem.MetadataUpdateTriggers.Apply(trigger => trigger.AfterUpdate(fileName, sourceMetadata));

                Search.Index(fileName, sourceMetadata, updateMetadata.Etag);

                if (isConflictResolved)
                {
                    ConflictArtifactManager.Delete(fileName);
                }

                Synchronizations.PublishFileNotification(fileName, FileChangeAction.Update);
			}
			catch (Exception ex)
			{
				if (ShouldAddExceptionToReport(ex))
				{
					report.Exception = ex;

					Log.WarnException(
						string.Format("Error was occurred during metadata synchronization of file '{0}' from {1}", fileName, sourceInfo), ex);
				}
			}
			finally
			{
				Synchronizations.FinishSynchronization(fileName, report, sourceInfo, sourceFileETag);                
			}

            Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Finish);

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
				Log.Debug("Metadata of file '{0}' was synchronized successfully from {1}", fileName, sourceInfo);
			}

            return GetMessageWithObject(report, HttpStatusCode.OK);
		}


		[HttpDelete]
        [RavenRoute("fs/{fileSystemName}/synchronization")]
        public HttpResponseMessage Delete(string fileName)
		{
            fileName = FileHeader.Canonize(fileName);

			var sourceInfo = GetSourceFileSystemInfo();
			var sourceFileETag = GetEtag();

            Log.Debug("Starting to delete a file '{0}' with ETag {1} from {2} because of synchronization", fileName, sourceFileETag, sourceInfo);

            var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.Delete);

			try
			{
				Storage.Batch(accessor =>
				{
                    Synchronizations.AssertFileIsNotBeingSynced(fileName);
                    FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceInfo, accessor);
				});

                SynchronizationTask.IncomingSynchronizationStarted(fileName, sourceInfo, sourceFileETag, SynchronizationType.Delete);

                Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(fileName, accessor));

				var localMetadata = Synchronizations.GetLocalMetadata(fileName);

				if (localMetadata != null)
				{
                    var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

                    bool isConflictResolved;

                    AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceInfo, out isConflictResolved);

                    Storage.Batch(accessor =>
                    {
                        Files.IndicateFileToDelete(fileName, null);

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

                    Synchronizations.PublishFileNotification(fileName, FileChangeAction.Delete);
				}
			}
			catch (Exception ex)
			{
				if (ShouldAddExceptionToReport(ex))
				{
					report.Exception = ex;

					Log.WarnException(string.Format("Error was occurred during deletion synchronization of file '{0}' from {1}", fileName, sourceInfo), ex);
				}
			}
			finally
			{
				Synchronizations.FinishSynchronization(fileName, report, sourceInfo, sourceFileETag);               
			}

            Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Finish);

			if (report.Exception == null)
			{
				Log.Debug("File '{0}' was deleted during synchronization from {1}", fileName, sourceInfo);
			}

            return GetMessageWithObject(report, HttpStatusCode.OK);
		}

		[HttpPatch]
        [RavenRoute("fs/{fileSystemName}/synchronization/Rename")]
        public HttpResponseMessage Rename(string fileName, string rename)
		{
            bool isConflictResolved = false;

            fileName = FileHeader.Canonize(fileName);
            rename = FileHeader.Canonize(rename);

			var sourceInfo = GetSourceFileSystemInfo();
			var sourceFileETag = GetEtag();
            var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

			Log.Debug("Starting to rename a file '{0}' to '{1}' with ETag {2} from {3} because of synchronization", fileName,
					  rename, sourceFileETag, sourceInfo);

            var report = new SynchronizationReport(fileName, sourceFileETag, SynchronizationType.Rename);

			try
			{
				Storage.Batch(accessor =>
				{
					Synchronizations.AssertFileIsNotBeingSynced(fileName);
                    FileLockManager.LockByCreatingSyncConfiguration(fileName, sourceInfo, accessor);
				});

                SynchronizationTask.IncomingSynchronizationStarted(fileName, sourceInfo, sourceFileETag, SynchronizationType.Rename);

                Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Start);

                Storage.Batch(accessor => StartupProceed(fileName, accessor));

				var localMetadata = Synchronizations.GetLocalMetadata(fileName);

                AssertConflictDetection(fileName, localMetadata, sourceMetadata, sourceInfo, out isConflictResolved);
                
				if (isConflictResolved)
                {
                    ConflictArtifactManager.Delete(fileName); 
                }
					
                Files.ExecuteRenameOperation(new RenameFileOperation
                {
                    FileSystem = FileSystem.Name,
                    Name = fileName,
                    Rename = rename,
                    MetadataAfterOperation = sourceMetadata.DropRenameMarkers()
                }, null);
			}
			catch (Exception ex)
			{
				if (ShouldAddExceptionToReport(ex))
				{
					report.Exception = ex;
					Log.WarnException(string.Format("Error was occurred during renaming synchronization of file '{0}' from {1}", fileName, sourceInfo), ex);
				}
			}
			finally
			{
				Synchronizations.FinishSynchronization(fileName, report, sourceInfo, sourceFileETag);               
			}

            Synchronizations.PublishSynchronizationNotification(fileName, sourceInfo, report.Type, SynchronizationAction.Finish);

            if (isConflictResolved)
            {
                Publisher.Publish(new ConflictNotification
                {
                    FileName = fileName,
                    Status = ConflictStatus.Resolved
                });
            }

			if (report.Exception == null)
				Log.Debug("File '{0}' was renamed to '{1}' during synchronization from {2}", fileName, rename, sourceInfo);

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
				.Deserialize<IEnumerable<Tuple<string, Etag>>>(new JsonTextReader(new StreamReader(contentStream)));


            var result = confirmingFiles.Select(x =>
            {
                string canonicalFilename = FileHeader.Canonize(x.Item1);
                return new SynchronizationConfirmation 
                {
                    FileName = canonicalFilename,
                    Status = CheckSynchronizedFileStatus(canonicalFilename, x.Item2)
                };
            });         

            return GetMessageWithObject(result)
                       .WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/Status")]
        public HttpResponseMessage Status(string fileName)
		{
            fileName = FileHeader.Canonize(fileName);

			var report = Synchronizations.GetSynchronizationReport(fileName);

            return GetMessageWithObject(report)
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

            return GetMessageWithObject(page, HttpStatusCode.OK)
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

            return GetMessageWithObject(result, HttpStatusCode.OK)
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

            return GetMessageWithObject(result, HttpStatusCode.OK)
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

            return GetMessageWithObject(result, HttpStatusCode.OK)
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

            return GetMessageWithObject(page, HttpStatusCode.OK)
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

                        accessor.UpdateFileMetadata(canonicalFilename, localMetadata, null);

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

                        accessor.UpdateFileMetadata(canonicalFilename, localMetadata, null);

                        // ConflictArtifactManager.Delete(canonicalFilename, accessor); - intentionally not deleting, conflict item will be removed when a remote file is put
					});

					Task.Run(() => SynchronizationTask.Execute(true));
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

			var localMetadata = Synchronizations.GetLocalMetadata(conflict.FileName);
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

			var localMetadata = Synchronizations.GetLocalMetadata(canonicalFilename);

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
			SourceSynchronizationInformation lastEtag= Synchronizations.GetLastSynchronization(from);

			Log.Debug("Got synchronization last ETag request from {0}: [{1}]", from, lastEtag);

            return GetMessageWithObject(lastEtag)
                       .WithNoCache();
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/IncrementLastETag")]
		public HttpResponseMessage IncrementLastETag(Guid sourceServerId, string sourceFileSystemUrl, string sourceFileETag)
		{
			Synchronizations.IncermentLastEtag(sourceServerId, sourceFileSystemUrl, sourceFileETag);

			return GetEmptyMessage();
		}

		private FileStatus CheckSynchronizedFileStatus(string filename, Etag etag)
		{
            var report = Synchronizations.GetSynchronizationReport(filename);
            if (report == null || report.FileETag != etag)
				return FileStatus.Unknown;

			return report.Exception == null ? FileStatus.Safe : FileStatus.Broken;
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
