using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Storage.Exceptions;
using Raven.Database.FileSystem.Util;
using Raven.Database.Plugins;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.Extensions;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Util;
using Raven.Client.FileSystem;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Controllers
{
    public class FilesController : BaseFileSystemApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/files")]
        public HttpResponseMessage Get([FromUri] string[] fileNames)
        {
            var list = new List<FileHeader>();

            var startsWith = GetQueryStringValue("startsWith");
            if (string.IsNullOrEmpty(startsWith) == false)
            {
                var matches = GetQueryStringValue("matches");

                var endsWithSlash = startsWith.EndsWith("/") || startsWith.EndsWith("\\");
                startsWith = FileHeader.Canonize(startsWith);
                if (endsWithSlash)
                    startsWith += "/";

                Storage.Batch(accessor =>
                {
                    var actualStart = 0;
                    var filesToSkip = Paging.Start;
                    int fileCount, matchedFiles = 0, addedFiles = 0;

                    do
                    {
                        fileCount = 0;

                        foreach (var file in accessor.GetFilesStartingWith(startsWith, actualStart, Paging.PageSize))
                        {
                            fileCount++;

                            var keyTest = file.FullPath.Substring(startsWith.Length);

                            if (WildcardMatcher.Matches(matches, keyTest) == false)
                                continue;

                            if (FileSystem.ReadTriggers.CanReadFile(file.FullPath, file.Metadata, ReadOperation.Load) == false)
                                continue;

                            matchedFiles++;

                            if (matchedFiles <= filesToSkip)
                                continue;

                            list.Add(file);
                            addedFiles++;
                        }

                        actualStart += Paging.PageSize;
                    }
                    while (fileCount > 0 && addedFiles < Paging.PageSize && actualStart > 0 && actualStart < int.MaxValue);
                });
            }
            else
            {
                if (fileNames != null && fileNames.Length > 0)
                {
                    Storage.Batch(accessor =>
                    {
                        foreach (var path in fileNames.Where(x => x != null).Select(FileHeader.Canonize))
                        {
                            var file = accessor.ReadFile(path);

                            if (file == null || file.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
                            {
                                list.Add(null);
                                continue;
                            }

                            list.Add(file);
                        }
                    });
                }
                else
                {
                    int results;
                    long durationInMs;
                    var keys = Search.Query(null, null, Paging.Start, Paging.PageSize, out results, out durationInMs);

                    Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));
                }
            }

            return GetMessageWithObject(list)
                .WithNoCache();
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/debug/files/count")]
        public HttpResponseMessage Count()
        {
            int FileCountFromStats = 0;
            int FileCount = 0;
            int TombstoneCount = 0;
            int RenameTombstoneCount = 0;
            int DeletingCount = 0;
            int DownloadingCount = 0;

            Storage.Batch(accessor =>
            {
                FileCountFromStats = accessor.GetFileCount();

                var fileHeaders = accessor.GetFilesAfter(Etag.Empty, int.MaxValue);

                foreach (var file in fileHeaders)
                {
                    if (file.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
                    {
                        if (file.Metadata.Keys.Contains(SynchronizationConstants.RavenRenameFile))
                        {
                            RenameTombstoneCount++;
                        }
                        else
                        {
                            TombstoneCount++;
                        }

                        continue;
                    }

                    FileCount++;

                    if (file.FullPath.EndsWith(RavenFileNameHelper.DownloadingFileSuffix))
                        DownloadingCount++;
                    else if (file.FullPath.EndsWith(RavenFileNameHelper.DeletingFileSuffix))
                        DeletingCount++;
                }
            });

            return GetMessageWithObject(new
            {
                FileCountFromStats = FileCountFromStats,
                FileCount = FileCount,
                TombstoneCount = TombstoneCount,
                RenameTombstoneCount = RenameTombstoneCount,
                DownloadingCount = DownloadingCount,
                DeletingCount = DeletingCount
            }).WithNoCache();
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Get(string name)
        {
            name = FileHeader.Canonize(name);
            FileAndPagesInformation fileAndPages = null;

            Storage.Batch(accessor => fileAndPages = accessor.GetFile(name, 0, 0));

            if (fileAndPages.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
            {
                if (log.IsDebugEnabled)
                    log.Debug("File '{0}' is not accessible to get (Raven-Delete-Marker set)", name);
                throw new HttpResponseException(HttpStatusCode.NotFound);
            }

            var readingStream = StorageStream.Reading(Storage, name);

            var filename = GetFileName(name, fileAndPages.Metadata);
            var result = StreamResult(filename, readingStream);

            var etag = new Etag(fileAndPages.Metadata.Value<string>(Constants.MetadataEtagField));
            fileAndPages.Metadata.Remove(Constants.MetadataEtagField);
            WriteHeaders(fileAndPages.Metadata, etag, result);

            if (log.IsDebugEnabled)
                log.Debug("File '{0}' with etag {1} is being retrieved.", name, etag);

            return result.WithNoCache();
        }

        private static string GetFileName(string name, RavenJObject metadata)
        {
            var revisionStatus = metadata.Value<string>(VersioningUtil.RavenFileRevisionStatus);
            if (revisionStatus == "Historical")
            {
                var stringSeparators = new[] { "/revisions/" };
                var nameSplitted = name.Split(stringSeparators, StringSplitOptions.None);
                if (nameSplitted.Length >= 2)
                {
                    var fileName = Path.GetFileName(nameSplitted[nameSplitted.Length - 2]);
                    return $"Revision {nameSplitted[nameSplitted.Length - 1]}, {fileName}";
                }
            }

            return Path.GetFileName(name);
        }

        [HttpDelete]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Delete(string name)
        {
            name = FileHeader.Canonize(name);

            using (FileSystem.FileLock.Lock())
            {
                Storage.Batch(accessor =>
                {
                    Synchronizations.AssertFileIsNotBeingSynced(name);

                    var fileAndPages = accessor.GetFile(name, 0, 0);

                    var metadata = fileAndPages.Metadata;

                    if (metadata == null)
                        throw new FileNotFoundException();

                    if (metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
                        throw new FileNotFoundException();

                    Historian.Update(name, metadata);
                    Files.IndicateFileToDelete(name, GetEtag());

                    if (name.EndsWith(RavenFileNameHelper.DownloadingFileSuffix) == false) // don't create a tombstone for .downloading file
                    {
                        Files.PutTombstone(name, metadata);
                        accessor.DeleteConfig(RavenFileNameHelper.ConflictConfigNameForFile(name)); // delete conflict item too
                    }
                });
            }

            SynchronizationTask.Context.NotifyAboutWork();

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpHead]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Head(string name)
        {
            name = FileHeader.Canonize(name);
            FileAndPagesInformation fileAndPages = null;

            Storage.Batch(accessor => fileAndPages = accessor.GetFile(name, 0, 0));

            if (fileAndPages.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
            {
                if (log.IsDebugEnabled)
                    log.Debug("Cannot get metadata of a file '{0}' because file was deleted", name);
                throw new FileNotFoundException();
            }

            var httpResponseMessage = GetEmptyMessage();

            var etag = new Etag(fileAndPages.Metadata.Value<string>(Constants.MetadataEtagField));
            fileAndPages.Metadata.Remove(Constants.MetadataEtagField);

            WriteHeaders(fileAndPages.Metadata, etag, httpResponseMessage);

            return httpResponseMessage;
        }

        private const string CopyPrefix = "copy/";

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Post(string name)
        {
            if (name.StartsWith(CopyPrefix))
            {
                var targetFileName = GetQueryStringValue("targetFilename");
                return Copy(name.Substring(CopyPrefix.Length), targetFileName);
            }

            name = FileHeader.Canonize(name);

            var metadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);
            var etag = GetEtag();

            using (FileSystem.FileLock.Lock())
            {
                Storage.Batch(accessor =>
                {
                    Synchronizations.AssertFileIsNotBeingSynced(name);

                    Historian.Update(name, metadata);
                    Files.UpdateMetadata(name, metadata, etag);
                });
            }

            SynchronizationTask.Context.NotifyAboutWork();

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/files-copy/{*name}")]
        public HttpResponseMessage Copy(string name, string targetFilename)
        {
            name = FileHeader.Canonize(name);
            targetFilename = FileHeader.Canonize(targetFilename);
            var etag = GetEtag();

            var retriesCount = 0;

            while (true)
            {
                try
                {
                    using (FileSystem.FileLock.Lock())
                    {
                        Storage.Batch(accessor =>
                        {
                            FileSystem.Synchronizations.AssertFileIsNotBeingSynced(name);

                            var existingFile = accessor.ReadFile(name);
                            if (existingFile == null || existingFile.Metadata.Value<bool>(SynchronizationConstants.RavenDeleteMarker))
                                throw new FileNotFoundException();

                            var renamingFile = accessor.ReadFile(targetFilename);
                            if (renamingFile != null && renamingFile.Metadata.Value<bool>(SynchronizationConstants.RavenDeleteMarker) == false)
                                throw new FileExistsException("Cannot copy because file " + targetFilename + " already exists");

                            var metadata = existingFile.Metadata;

                            if (etag != null && existingFile.Etag != etag)
                                throw new ConcurrencyException("Operation attempted on file '" + name + "' using a non current etag")
                                {
                                    ActualETag = existingFile.Etag,
                                    ExpectedETag = etag
                                };

                            Historian.UpdateLastModified(metadata);

                            var operation = new CopyFileOperation
                            {
                                FileSystem = FileSystem.Name,
                                SourceFilename = name,
                                TargetFilename = targetFilename,
                                MetadataAfterOperation = metadata
                            };

                            accessor.SetConfig(RavenFileNameHelper.CopyOperationConfigNameForFile(name, targetFilename), JsonExtensions.ToJObject(operation));
                            var configName = RavenFileNameHelper.CopyOperationConfigNameForFile(operation.SourceFilename, operation.TargetFilename);
                            Files.AssertPutOperationNotVetoed(operation.TargetFilename, operation.MetadataAfterOperation);

                            var targetTombstrone = accessor.ReadFile(operation.TargetFilename);

                            if (targetTombstrone != null &&
                                targetTombstrone.Metadata[SynchronizationConstants.RavenDeleteMarker] != null)
                            {
                                // if there is a tombstone delete it
                                accessor.Delete(targetTombstrone.FullPath);
                            }

                            FileSystem.PutTriggers.Apply(trigger => trigger.OnPut(operation.TargetFilename, operation.MetadataAfterOperation));

                            accessor.CopyFile(operation.SourceFilename, operation.TargetFilename, true);
                            var putResult = accessor.UpdateFileMetadata(operation.TargetFilename, operation.MetadataAfterOperation, null);

                            FileSystem.PutTriggers.Apply(trigger => trigger.AfterPut(operation.TargetFilename, null, operation.MetadataAfterOperation));

                            accessor.DeleteConfig(configName);

                            Search.Index(operation.TargetFilename, operation.MetadataAfterOperation, putResult.Etag);


                            Publisher.Publish(new ConfigurationChangeNotification {Name = configName, Action = ConfigurationChangeAction.Set});
                            Publisher.Publish(new FileChangeNotification {File = operation.TargetFilename, Action = FileChangeAction.Add});


                        });
                    }

                    break;
                }
                catch (ConcurrencyException)
                {
                    // due to IncrementUsageCount performed concurrently on Voron storage
                    // Esent deals with that much better using Api.EscrowUpdate

                    if (retriesCount++ > 100)
                        throw;

                    Thread.Sleep(new Random().Next(1, 13));
                }
            }

            if (Log.IsDebugEnabled)
                Log.Debug("File '{0}' was copied to '{1}'", name, targetFilename);

            SynchronizationTask.Context.NotifyAboutWork();

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpPatch]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Patch(string name, string rename)
        {
            name = FileHeader.Canonize(name);
            rename = FileHeader.Canonize(rename);
            var etag = GetEtag();

            if (rename.Length > SystemParameters.KeyMost)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("File '{0}' was not renamed to '{1}' due to illegal name length", name, rename);
                return GetMessageWithString(string.Format("File '{0}' was not renamed to '{1}' due to illegal name length", name, rename), HttpStatusCode.BadRequest);
            }

            Storage.Batch(accessor =>
            {
                FileSystem.Synchronizations.AssertFileIsNotBeingSynced(name);

                var existingFile = accessor.ReadFile(name);
                if (existingFile == null || existingFile.Metadata.Value<bool>(SynchronizationConstants.RavenDeleteMarker))
                    throw new FileNotFoundException();

                var renamingFile = accessor.ReadFile(rename);
                if (renamingFile != null && renamingFile.Metadata.Value<bool>(SynchronizationConstants.RavenDeleteMarker) == false)
                    throw new FileExistsException("Cannot rename because file " + rename + " already exists");

                var metadata = existingFile.Metadata;

                if (etag != null && existingFile.Etag != etag)
                    throw new ConcurrencyException("Operation attempted on file '" + name + "' using a non current etag")
                    {
                        ActualETag = existingFile.Etag,
                        ExpectedETag = etag
                    };

                Historian.UpdateLastModified(metadata);

                var operation = new RenameFileOperation(name, rename, existingFile.Etag, metadata);

                accessor.SetConfig(RavenFileNameHelper.RenameOperationConfigNameForFile(name), JsonExtensions.ToJObject(operation));
                accessor.PulseTransaction(); // commit rename operation config

                Files.ExecuteRenameOperation(operation);
            });

            if (Log.IsDebugEnabled)
                Log.Debug("File '{0}' was renamed to '{1}'", name, rename);

            SynchronizationTask.Context.NotifyAboutWork();

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpPut]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public async Task<HttpResponseMessage> Put(string name, bool preserveTimestamps = false)
        {
            var metadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);
            var etag = GetEtag();

            if (name.Length > SystemParameters.KeyMost)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("File '{0}' was not created due to illegal name length", name);
                return GetMessageWithString(string.Format("File '{0}' was not created due to illegal name length", name), HttpStatusCode.BadRequest);
            }

            if (Log.IsDebugEnabled)
                Log.Debug("Putting file '{0}'", name);

            var options = new FileActions.PutOperationOptions();

            long contentSize;
            if (long.TryParse(GetHeader(Constants.FileSystem.RavenFsSize), out contentSize))
                options.ContentSize = contentSize;

            DateTimeOffset lastModified;
            if (DateTimeOffset.TryParse(GetHeader(Constants.RavenLastModified), out lastModified))
                options.LastModified = lastModified;

            options.PreserveTimestamps = preserveTimestamps;
            options.ContentLength = Request.Content.Headers.ContentLength;
            options.TransferEncodingChunked = Request.Headers.TransferEncodingChunked ?? false;

            await FileSystem.Files.PutAsync(name, etag, metadata, () => Request.Content.ReadAsStreamAsync(), options).ConfigureAwait(false);

            SynchronizationTask.Context.NotifyAboutWork();

            if (Log.IsDebugEnabled)
                Log.Debug("File '{0}' has been put", name);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/files/touch")]
        public HttpResponseMessage TouchFiles()
        {
            var startEtag = GetEtagFromQueryString();
            var pageSize = GetPageSize(1024);

            Etag lastEtag = null;
            long touched = 0;
            long skipped = 0;

            FileUpdateResult lastTouch = null;

            for (int i = 0; i < 10; i++)
            {

                try
                {
                    Storage.Batch(accessor =>
                    {
                        var fileHeaders = accessor.GetFilesAfter(startEtag, pageSize * 2);

                        foreach (var file in fileHeaders)
                        {
                            lastEtag = file.Etag;

                            if (file.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
                            {
                                skipped++;
                                continue;
                            }

                            if (file.FullPath.EndsWith(RavenFileNameHelper.DownloadingFileSuffix))
                            {
                                skipped++;
                                continue;
                            }

                            if (file.FullPath.EndsWith(RavenFileNameHelper.DeletingFileSuffix))
                            {
                                skipped++;
                                continue;
                            }

                            lastTouch = accessor.TouchFile(file.FullPath, null);

                            touched++;

                            if (touched >= pageSize)
                                break;
                        }
                    });

                    break;
                }
                catch (ConcurrencyException)
                {
                    // retry

                    lastEtag = null;
                    touched = 0;
                    skipped = 0;

                    Thread.Sleep(500);
                }
            }


            if (touched > 0)
                SynchronizationTask.Context.NotifyAboutWork();
            
            return GetMessageWithObject(new TouchFilesResult
            {
                NumberOfProcessedFiles = touched,
                LastProcessedFileEtag = lastEtag,
                NumberOfFilteredFiles = skipped,
                LastEtagAfterTouch = lastTouch?.Etag
            }).WithNoCache();
        }
    }
}
