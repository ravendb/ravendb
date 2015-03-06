using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Database.Plugins;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.FileSystem.Controllers
{
    public class FilesController : RavenFsApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/files")]
        public HttpResponseMessage Get()
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

                            if (FileSystem.ReadTriggers.CanReadFile(FileHeader.Canonize(file.FullPath), file.Metadata, ReadOperation.Load) == false) 
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
				int results;
				var keys = Search.Query(null, null, Paging.Start, Paging.PageSize, out results);
				Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));
			}

			return GetMessageWithObject(list)
				.WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Get(string name)
		{
            name = FileHeader.Canonize(name);
			FileAndPagesInformation fileAndPages = null;
			try
			{
				Storage.Batch(accessor => fileAndPages = accessor.GetFile(name, 0, 0));
			}
			catch (FileNotFoundException)
			{
				log.Debug("File '{0}' was not found", name);
				throw new HttpResponseException(HttpStatusCode.NotFound);
			}

            if (fileAndPages.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
			{
				log.Debug("File '{0}' is not accessible to get (Raven-Delete-Marker set)", name);
				throw new HttpResponseException(HttpStatusCode.NotFound);
			}

            var readingStream = StorageStream.Reading(Storage, name);

            var filename = Path.GetFileName(name);
            var result = StreamResult(filename, readingStream);

            var etag = new Etag(fileAndPages.Metadata.Value<string>(Constants.MetadataEtagField));
            fileAndPages.Metadata.Remove(Constants.MetadataEtagField);
            WriteHeaders(fileAndPages.Metadata, etag, result);

            log.Debug("File '{0}' with etag {1} is being retrieved.", name, etag);

            return result.WithNoCache();
		}

		[HttpDelete]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Delete(string name)
		{
            name = FileHeader.Canonize(name);

			try
			{
				Storage.Batch(accessor =>
				{
					AssertFileIsNotBeingSynced(name, accessor, true);

					var fileAndPages = accessor.GetFile(name, 0, 0);

					var metadata = fileAndPages.Metadata;

					if (metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
					{
						throw new FileNotFoundException();
					}

					StorageOperationsTask.IndicateFileToDelete(name, GetEtag());

					if (!name.EndsWith(RavenFileNameHelper.DownloadingFileSuffix) &&
					    // don't create a tombstone for .downloading file
					    metadata != null) // and if file didn't exist
					{
						var tombstoneMetadata = new RavenJObject
						{
							{
								SynchronizationConstants.RavenSynchronizationHistory, metadata[SynchronizationConstants.RavenSynchronizationHistory]
							},
							{
								SynchronizationConstants.RavenSynchronizationVersion, metadata[SynchronizationConstants.RavenSynchronizationVersion]
							},
							{
								SynchronizationConstants.RavenSynchronizationSource, metadata[SynchronizationConstants.RavenSynchronizationSource]
							}
						}.WithDeleteMarker();

						Historian.UpdateLastModified(tombstoneMetadata);

						accessor.PutFile(name, 0, tombstoneMetadata, true);
						accessor.DeleteConfig(RavenFileNameHelper.ConflictConfigNameForFile(name)); // delete conflict item too
					}
				});
			}
			catch (FileNotFoundException)
			{
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			}
			catch (ConcurrencyException ex)
			{
				throw ConcurrencyResponseException(ex);
			}

			Publisher.Publish(new FileChangeNotification { File = FilePathTools.Cannoicalise(name), Action = FileChangeAction.Delete });
			log.Debug("File '{0}' was deleted", name);

			FileSystem.Synchronization.StartSynchronizeDestinationsInBackground();

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpHead]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Head(string name)
		{
            name = FileHeader.Canonize(name);
			FileAndPagesInformation fileAndPages = null;
			try
			{
				Storage.Batch(accessor => fileAndPages = accessor.GetFile(name, 0, 0));
			}
			catch (FileNotFoundException)
			{
				log.Debug("Cannot get metadata of a file '{0}' because file was not found", name);
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			}

			if (fileAndPages.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
			{
				log.Debug("Cannot get metadata of a file '{0}' because file was deleted", name);
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			}
            
			var httpResponseMessage = GetEmptyMessage();

            var etag = new Etag(fileAndPages.Metadata.Value<string>(Constants.MetadataEtagField));
            fileAndPages.Metadata.Remove(Constants.MetadataEtagField);

            WriteHeaders(fileAndPages.Metadata, etag, httpResponseMessage);

			return httpResponseMessage;
		}

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/files/metadata")]
        public HttpResponseMessage Metadata([FromUri] string[] fileNames)
        {
            if (fileNames == null || fileNames.Length == 0)
            {
                log.Debug("'fileNames' parameter should have a value.");
                return GetEmptyMessage(HttpStatusCode.BadRequest);
            }

            var ravenPaths = fileNames.Where(x => x != null).Select(FileHeader.Canonize);

            var list = new List<FileHeader>();
            Storage.Batch(accessor => list.AddRange(ravenPaths.Select(accessor.ReadFile).Where(x => x != null)));

            return GetMessageWithObject(list)
                       .WithNoCache();
        }

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Post(string name)
		{
            name = FileHeader.Canonize(name);

            var metadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);

            Historian.UpdateLastModified(metadata);
            Historian.Update(name, metadata);

			FileOperationResult updateMetadata = null;

            try
            {
		        Storage.Batch(accessor =>
		        {
			        AssertFileIsNotBeingSynced(name, accessor, true);
			        updateMetadata = accessor.UpdateFileMetadata(name, metadata, GetEtag());
		        });
            }
            catch (FileNotFoundException)
            {
                log.Debug("Cannot update metadata because file '{0}' was not found", name);
                return GetEmptyMessage(HttpStatusCode.NotFound);
            }
			catch (ConcurrencyException ex)
			{
				throw ConcurrencyResponseException(ex);
			}

            Search.Index(name, metadata, updateMetadata.Etag);

            Publisher.Publish(new FileChangeNotification { File = FilePathTools.Cannoicalise(name), Action = FileChangeAction.Update });

			FileSystem.Synchronization.StartSynchronizeDestinationsInBackground();

            log.Debug("Metadata of a file '{0}' was updated", name);

            //Hack needed by jquery on the client side. We need to find a better solution for this
            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPatch]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Patch(string name, string rename)
		{
            name = FileHeader.Canonize(name);
            rename = FileHeader.Canonize(rename);
			var etag = GetEtag();

			try
			{
				Storage.Batch(accessor =>
				{
					AssertFileIsNotBeingSynced(name, accessor, true);

					var metadata = accessor.GetFile(name, 0, 0).Metadata;
					if (metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
					{
						throw new FileNotFoundException();
					}

					var existingFile = accessor.ReadFile(rename);
					if (existingFile != null && !existingFile.Metadata.ContainsKey(SynchronizationConstants.RavenDeleteMarker))
					{
						throw new HttpResponseException(
							Request.CreateResponse(HttpStatusCode.Forbidden,
								new InvalidOperationException("Cannot rename because file " + rename + " already exists")));
					}

					Historian.UpdateLastModified(metadata);

					var operation = new RenameFileOperation
					{
						FileSystem = FileSystem.Name,
						Name = name,
						Rename = rename,
						MetadataAfterOperation = metadata
					};
					//TODO arek - need to ensure that config will be deleted if there is ConcurrencyException
					accessor.SetConfig(RavenFileNameHelper.RenameOperationConfigNameForFile(name), JsonExtensions.ToJObject(operation));
					accessor.PulseTransaction(); // commit rename operation config

					StorageOperationsTask.RenameFile(operation, etag);
				});
			}
			catch (FileNotFoundException)
			{
				log.Debug("Cannot rename a file '{0}' to '{1}' because a file was not found", name, rename);
				return GetEmptyMessage(HttpStatusCode.NotFound);
			}
			catch (ConcurrencyException ex)
			{
				throw ConcurrencyResponseException(ex);
			}

			log.Debug("File '{0}' was renamed to '{1}'", name, rename);

			FileSystem.Synchronization.StartSynchronizeDestinationsInBackground();

            return GetMessageWithString("", HttpStatusCode.NoContent);
		}

		[HttpPut]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public async Task<HttpResponseMessage> Put(string name, string uploadId = null, bool preserveTimestamps = false)
		{
			try
			{
				var metadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);
				var etag = GetEtag();

				var options = new FileActions.PutOperationOptions();

				Guid uploadIdentifier;
				if (uploadId != null && Guid.TryParse(uploadId, out uploadIdentifier))
					options.UploadId = uploadIdentifier;

				var sizeHeader = GetHeader("RavenFS-size");
				long contentSize;
				if (long.TryParse(sizeHeader, out contentSize))
					options.ContentSize = contentSize;

				var lastModifiedHeader = GetHeader(Constants.RavenLastModified);
				DateTimeOffset lastModified;
				if (lastModifiedHeader != null && DateTimeOffset.TryParse(lastModifiedHeader, out lastModified))
					options.LastModified = lastModified;

				options.PreserveTimestamps = preserveTimestamps;
				options.ContentLength = Request.Content.Headers.ContentLength;
				options.TransferEncodingChunked = Request.Headers.TransferEncodingChunked ?? false;

				await FileSystem.Files.PutAsync(name, etag, metadata, () => Request.Content.ReadAsStreamAsync(), options);
			}
			catch (Exception ex)
			{
				var concurrencyException = ex as ConcurrencyException;
				if (concurrencyException != null)
					throw ConcurrencyResponseException(concurrencyException);

				var synchronizationException = ex as SynchronizationException;
				if (synchronizationException != null)
					throw new HttpResponseException(Request.CreateResponse((HttpStatusCode)420, synchronizationException));

				throw;
			}

			return GetEmptyMessage(HttpStatusCode.Created);
		}
	}
}
