using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Util;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;

namespace Raven.Database.FileSystem.Controllers
{
    public class SynchronizationController : BaseFileSystemApiController
    {
        private static new readonly ILog Log = LogManager.GetCurrentClassLogger();

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/ToDestinations")]
        public async Task<HttpResponseMessage> ToDestinations(bool forceSyncingAll)
        {
            var tasks = SynchronizationTask.Execute(forceSyncingAll);

            var result = new List<DestinationSyncResult>();

            foreach (var task in tasks)
            {
                result.Add(await SynchronizationTask.CreateDestinationResult(task.Key, await task.Value.ConfigureAwait(false)).ConfigureAwait(false));
            }

            return GetMessageWithObject(result.ToArray());
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/ToDestination")]
        public async Task<HttpResponseMessage> ToDestination(string destination, bool forceSyncingAll)
        {
            var result = await SynchronizationTask.SynchronizeDestinationAsync(destination + "/fs/" + FileSystemName, forceSyncingAll).ConfigureAwait(false);
            
            return GetMessageWithObject(result);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/start/{*filename}")]
        public async Task<HttpResponseMessage> Start(string filename)
        {
            var canonicalFilename = FileHeader.Canonize(filename);

            var destination = await ReadJsonObjectAsync<SynchronizationDestination>().ConfigureAwait(false);

            if (Log.IsDebugEnabled)
            Log.Debug("Starting to synchronize a file '{0}' to {1}", canonicalFilename, destination.Url);

            var result = await SynchronizationTask.SynchronizeFileToAsync(canonicalFilename, destination).ConfigureAwait(false);

            return GetMessageWithObject(result);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/MultipartProceed")]
        public async Task<HttpResponseMessage> MultipartProceed()
        {
            bool skipRdc;
            bool.TryParse(QueryString["skip-rdc"], out skipRdc);

            if (!Request.Content.IsMimeMultipartContent())
                throw new HttpResponseException(HttpStatusCode.UnsupportedMediaType);

            var fileName = FileHeader.Canonize(Request.Headers.GetValues(SyncingMultipartConstants.FileName).FirstOrDefault());

            var sourceInfo = GetSourceFileSystemInfo();
            var sourceFileETag = GetEtag();
            var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);
            if (Log.IsDebugEnabled)
            Log.Debug("Starting to process multipart synchronization request of a file '{0}' with ETag {1} from {2}", fileName, sourceFileETag, sourceInfo);

            var report = await new SynchronizationBehavior(fileName, sourceFileETag, sourceMetadata, sourceInfo, skipRdc ? SynchronizationType.ContentUpdateNoRDC : SynchronizationType.ContentUpdate, FileSystem)
                            {
                                MultipartContent = Request.Content
            }.Execute().ConfigureAwait(false);

            if (report.Exception == null)
            {
                if (Log.IsDebugEnabled)
                Log.Debug(
                    "File '{0}' was synchronized successfully from {1}. {2} bytes were transfered and {3} bytes copied. Need list length was {4}",
                    fileName, sourceInfo, report.BytesTransfered, report.BytesCopied, report.NeedListLength);
            }
            else
            {
                Log.WarnException(string.Format("Error has occurred during synchronization of a file '{0}' from {1}", fileName, sourceInfo), report.Exception);
            }

            return GetMessageWithObject(report);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/UpdateMetadata/{*fileName}")]
        public async Task<HttpResponseMessage> UpdateMetadata(string fileName)
        {
            fileName = FileHeader.Canonize(fileName);

            var sourceInfo = GetSourceFileSystemInfo();
            var sourceFileETag = GetEtag();
            var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

            if (Log.IsDebugEnabled)
            Log.Debug("Starting to update a metadata of file '{0}' with ETag {1} from {2} because of synchronization", fileName, sourceFileETag, sourceInfo);

            var report = await new SynchronizationBehavior(fileName, sourceFileETag, sourceMetadata, sourceInfo, SynchronizationType.MetadataUpdate, FileSystem)
                .Execute().ConfigureAwait(false);

            if (Log.IsDebugEnabled && report.Exception == null)
                Log.Debug("Metadata of file '{0}' was synchronized successfully from {1}", fileName, sourceInfo);

            return GetMessageWithObject(report);
        }


        [HttpDelete]
        [RavenRoute("fs/{fileSystemName}/synchronization")]
        public async Task<HttpResponseMessage> Delete(string fileName)
        {
            fileName = FileHeader.Canonize(fileName);

            var sourceInfo = GetSourceFileSystemInfo();
            var sourceFileETag = GetEtag();
            var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

            if (Log.IsDebugEnabled)
            Log.Debug("Starting to delete a file '{0}' with ETag {1} from {2} because of synchronization", fileName, sourceFileETag, sourceInfo);

            var report = await new SynchronizationBehavior(fileName, sourceFileETag, sourceMetadata, sourceInfo, SynchronizationType.Delete, FileSystem)
                .Execute().ConfigureAwait(false);

            if (Log.IsDebugEnabled && report.Exception == null)
                Log.Debug("File '{0}' was deleted during synchronization from {1}", fileName, sourceInfo);

            return GetMessageWithObject(report);
        }

        [HttpPatch]
        [RavenRoute("fs/{fileSystemName}/synchronization/Rename")]
        public async Task<HttpResponseMessage> Rename(string fileName, string rename)
        {
            fileName = FileHeader.Canonize(fileName);
            rename = FileHeader.Canonize(rename);

            var sourceInfo = GetSourceFileSystemInfo();
            var sourceFileEtag = GetEtag();
            var sourceMetadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

            if (Log.IsDebugEnabled)
            Log.Debug("Starting to rename a file '{0}' to '{1}' with ETag {2} from {3} because of synchronization", fileName,
                      rename, sourceFileEtag, sourceInfo);

            var report = await new SynchronizationBehavior(fileName, sourceFileEtag, sourceMetadata, sourceInfo, SynchronizationType.Rename, FileSystem)
            {
                Rename = rename
            }.Execute().ConfigureAwait(false);

            if (Log.IsDebugEnabled && report.Exception == null)
                Log.Debug("File '{0}' was renamed to '{1}' during synchronization from {2}", fileName, rename, sourceInfo);

            return GetMessageWithObject(report);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/Confirm")]
        public async Task<HttpResponseMessage> Confirm()
        {
            var contentStream = await Request.Content.ReadAsStreamAsync().ConfigureAwait(false);

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
                int totalCount = 0;
                var configs = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncResultNamePrefix,
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
                int totalCount;
                var values = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.ConflictConfigNamePrefix, Paging.Start, Paging.PageSize, out totalCount);

                var conflicts = values.Select(x => JsonExtensions.JsonDeserialization<ConflictItem>(x));

                page = new ItemsPage<ConflictItem>(conflicts, totalCount);
            });

            return GetMessageWithObject(page, HttpStatusCode.OK)
                       .WithNoCache();		
        }

        [HttpPatch]
        [RavenRoute("fs/{fileSystemName}/synchronization/ResolveConflicts")]
        public HttpResponseMessage ResolveConflicts(ConflictResolutionStrategy strategy)
        {
            IList<ConflictItem> conflicts = null;
            Storage.Batch(accessor =>
            {
                int totalCount;
                var values = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.ConflictConfigNamePrefix, 0, int.MaxValue, out totalCount);
                conflicts = values.Select(x => JsonExtensions.JsonDeserialization<ConflictItem>(x)).ToList();
            });

            foreach (var conflict in conflicts)
            {
                ResolveConflict(conflict.FileName, strategy);
            }
            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpPatch]
        [RavenRoute("fs/{fileSystemName}/synchronization/ResolveConflict/{*filename}")]
        public HttpResponseMessage ResolveConflict(string filename, ConflictResolutionStrategy strategy)
        {
            var canonicalFilename = FileHeader.Canonize(filename);

            if (Log.IsDebugEnabled)
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
                        FileName = canonicalFilename,
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
                        accessor.SetConfig(RavenFileNameHelper.ConflictConfigNameForFile(canonicalFilename), JsonExtensions.ToJObject(conflict));

                        // ConflictArtifactManager.Delete(canonicalFilename, accessor); - intentionally not deleting, conflict item will be removed when a remote file is put
                    });

                    SynchronizationTask.Context.NotifyAboutWork();

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
            var conflict = await ReadJsonObjectAsync<ConflictItem>().ConfigureAwait(false);

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

            var contentStream = await Request.Content.ReadAsStreamAsync().ConfigureAwait(false);

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
                FileName = canonicalFilename, 
                SourceServerUrl = remoteServerUrl,
                Status = ConflictStatus.Detected,
                RemoteFileHeader = new FileHeader(canonicalFilename, remoteMetadata)
            });

            if (Log.IsDebugEnabled)
            Log.Debug("Conflict applied for a file '{0}' (remote version: {1}, remote server id: {2}).", filename, remoteVersion, remoteServerId);

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/synchronization/LastSynchronization")]
        public HttpResponseMessage LastSynchronization(Guid from)
        {
            SourceSynchronizationInformation lastEtag= Synchronizations.GetLastSynchronization(from);

            if (Log.IsDebugEnabled)
            Log.Debug("Got synchronization last ETag request from {0}: [{1}]", from, lastEtag);

            return GetMessageWithObject(lastEtag)
                       .WithNoCache();
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/synchronization/IncrementLastETag")]
        public HttpResponseMessage IncrementLastETag(Guid sourceServerId, string sourceFileSystemUrl, string sourceFileETag)
        {
            bool force;
            bool.TryParse(QueryString["force"], out force);

            Synchronizations.IncrementLastEtag(sourceServerId, sourceFileSystemUrl, sourceFileETag, force);

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
