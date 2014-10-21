using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Extensions;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Server.RavenFS.Controllers
{
    public class FilesController : RavenFsApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

		[HttpGet]
        [Route("fs/{fileSystemName}/files")]
        public HttpResponseMessage Get()
		{
            int results;
            var keys = Search.Query(null, null, Paging.Start, Paging.PageSize, out results);

            var list = new List<FileHeader>();
            Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));

            return this.GetMessageWithObject(list, HttpStatusCode.OK)
                       .WithNoCache();
		}

		[HttpGet]
        [Route("fs/{fileSystemName}/files/{*name}")]
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
        [Route("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Delete(string name)
		{
            name = FileHeader.Canonize(name);

			try
			{
				ConcurrencyAwareExecutor.Execute(() => Storage.Batch(accessor =>
				{
					AssertFileIsNotBeingSynced(name, accessor, true);

					var fileAndPages = accessor.GetFile(name, 0, 0);

					var metadata = fileAndPages.Metadata;

					if (metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
					{
						throw new FileNotFoundException();
					}

					StorageOperationsTask.IndicateFileToDelete(name);

					if ( !name.EndsWith(RavenFileNameHelper.DownloadingFileSuffix) &&
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
                        accessor.DeleteConfig(RavenFileNameHelper.ConflictConfigNameForFile(name));
						// delete conflict item too
					}
				}), ConcurrencyResponseException);
			}
			catch (FileNotFoundException)
			{
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			}

			Publisher.Publish(new FileChangeNotification { File = FilePathTools.Cannoicalise(name), Action = FileChangeAction.Delete });
			log.Debug("File '{0}' was deleted", name);

			StartSynchronizeDestinationsInBackground();

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpHead]
        [Route("fs/{fileSystemName}/files/{*name}")]
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
        [Route("fs/{fileSystemName}/files/metadata")]
        public HttpResponseMessage Metadata([FromUri] string[] fileNames)
        {
            if (fileNames == null || fileNames.Length == 0)
            {
                log.Debug("'fileNames' parameter should have a value.");
                return GetEmptyMessage(HttpStatusCode.BadRequest);
            }

            var ravenPaths = fileNames.Where(x => x != null).Select(x => FileHeader.Canonize(x));

            var list = new List<FileHeader>();
            Storage.Batch(accessor => list.AddRange(ravenPaths.Select(accessor.ReadFile).Where(x => x != null)));

            return this.GetMessageWithObject(list, HttpStatusCode.OK)
                       .WithNoCache();
        }

		[HttpPost]
        [Route("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Post(string name)
		{
            name = FileHeader.Canonize(name);

            var headers = this.GetFilteredMetadataFromHeaders(InnerHeaders);

            Historian.UpdateLastModified(headers);
            Historian.Update(name, headers);

            try
            {
                ConcurrencyAwareExecutor.Execute(() =>
                                                 Storage.Batch(accessor =>
                                                 {
                                                     AssertFileIsNotBeingSynced(name, accessor, true);
                                                     accessor.UpdateFileMetadata(name, headers);
                                                 }), ConcurrencyResponseException);
            }
            catch (FileNotFoundException)
            {
                log.Debug("Cannot update metadata because file '{0}' was not found", name);
                return GetEmptyMessage(HttpStatusCode.NotFound);
            }

            Search.Index(name, headers);

            Publisher.Publish(new FileChangeNotification { File = FilePathTools.Cannoicalise(name), Action = FileChangeAction.Update });

            StartSynchronizeDestinationsInBackground();

            log.Debug("Metadata of a file '{0}' was updated", name);

            //Hack needed by jquery on the client side. We need to find a better solution for this
            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPatch]
        [Route("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Patch(string name, string rename)
		{
            name = FileHeader.Canonize(name);
            rename = FileHeader.Canonize(rename);

			try
			{
				ConcurrencyAwareExecutor.Execute(() =>
					Storage.Batch(accessor =>
					{
						AssertFileIsNotBeingSynced(name, accessor, true);

						var metadata = accessor.GetFile(name, 0, 0).Metadata;
						if (metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
						{
							throw new FileNotFoundException();
						}

						var existingHeader = accessor.ReadFile(rename);
						if (existingHeader != null && !existingHeader.Metadata.ContainsKey(SynchronizationConstants.RavenDeleteMarker))
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

                        accessor.SetConfig(RavenFileNameHelper.RenameOperationConfigNameForFile(name), JsonExtensions.ToJObject(operation));
                        accessor.PulseTransaction(); // commit rename operation config

                        StorageOperationsTask.RenameFile(operation);
					}), ConcurrencyResponseException);
			}
			catch (FileNotFoundException)
			{
				log.Debug("Cannot rename a file '{0}' to '{1}' because a file was not found", name, rename);
                return GetEmptyMessage(HttpStatusCode.NotFound);
			}

			log.Debug("File '{0}' was renamed to '{1}'", name, rename);

			StartSynchronizeDestinationsInBackground();

            return GetMessageWithString("", HttpStatusCode.NoContent);
		}

		[HttpPut]
        [Route("fs/{fileSystemName}/files/{*name}")]
		public async Task<HttpResponseMessage> Put(string name, string uploadId = null)
		{         
			try
			{
                FileSystem.MetricsCounters.FilesPerSecond.Mark();

                name = FileHeader.Canonize(name);

                var headers = this.GetFilteredMetadataFromHeaders(InnerHeaders);

                Historian.UpdateLastModified(headers);


                var now = DateTimeOffset.UtcNow;
                headers[Constants.RavenCreationDate] = now;
                headers[Constants.CreationDate] = now.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture); // TODO: To keep current filesystems working. We should remove when adding a new migration. 
                Historian.Update(name, headers);

                SynchronizationTask.Cancel(name);

                long? size = -1;
                ConcurrencyAwareExecutor.Execute(() => Storage.Batch(accessor =>
                {
                    AssertFileIsNotBeingSynced(name, accessor, true);
                    StorageOperationsTask.IndicateFileToDelete(name);

                    var contentLength = Request.Content.Headers.ContentLength;
                    var sizeHeader = GetHeader("RavenFS-size");

                    long sizeForParse;
                    if (contentLength == 0 || long.TryParse(sizeHeader, out sizeForParse) == false)
                    {
                        size = contentLength;
                        if (Request.Headers.TransferEncodingChunked ?? false)
                        {
                            size = null;
                        }
                    }
                    else
                    {
                        size = sizeForParse;
                    }

                    accessor.PutFile(name, size, headers);

                    Search.Index(name, headers);                  
                }));

                log.Debug("Inserted a new file '{0}' with ETag {1}", name, headers.Value<Guid>(Constants.MetadataEtagField));

                using (var contentStream = await Request.Content.ReadAsStreamAsync())
                using (var readFileToDatabase = new ReadFileToDatabase(BufferPool, Storage, contentStream, name))
                {
                    await readFileToDatabase.Execute();                
   
                    if ( readFileToDatabase.TotalSizeRead != size )
                    {
                        Storage.Batch(accessor => StorageOperationsTask.IndicateFileToDelete(name));
                        throw new HttpResponseException(HttpStatusCode.BadRequest);
                    }                        

                    Historian.UpdateLastModified(headers); // update with the final file size

                    log.Debug("File '{0}' was uploaded. Starting to update file metadata and indexes", name);

                    headers["Content-MD5"] = readFileToDatabase.FileHash;                    

                    Storage.Batch(accessor => accessor.UpdateFileMetadata(name, headers));

                    int totalSizeRead = readFileToDatabase.TotalSizeRead;
                    headers["Content-Length"] = totalSizeRead.ToString(CultureInfo.InvariantCulture);
                    
                    Search.Index(name, headers);
                    Publisher.Publish(new FileChangeNotification { Action = FileChangeAction.Add, File = FilePathTools.Cannoicalise(name) });

                    log.Debug("Updates of '{0}' metadata and indexes were finished. New file ETag is {1}", name, headers.Value<Guid>(Constants.MetadataEtagField));

                    StartSynchronizeDestinationsInBackground();
                }
			}
			catch (Exception ex)
			{
				if (uploadId != null)
				{
					Guid uploadIdentifier;
					if (Guid.TryParse(uploadId, out uploadIdentifier))
					{
						Publisher.Publish(new CancellationNotification { UploadId = uploadIdentifier, File = name });
					}
				}

				log.WarnException(string.Format("Failed to upload a file '{0}'", name), ex);

				var concurrencyException = ex as ConcurrencyException;
				if (concurrencyException != null)
				{
					throw ConcurrencyResponseException(concurrencyException);
				}

				throw;
			}

            return GetEmptyMessage(HttpStatusCode.Created);
		}

		private void StartSynchronizeDestinationsInBackground()
		{
			Task.Factory.StartNew(async () => await SynchronizationTask.SynchronizeDestinationsAsync(), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
		}

		private class ReadFileToDatabase : IDisposable
		{
			private readonly byte[] buffer;
			private readonly BufferPool bufferPool;
			private readonly string filename;
			private readonly Stream inputStream;
			private readonly ITransactionalStorage storage;
			private readonly IHashEncryptor md5Hasher;
			public int TotalSizeRead;
			private int pos;

			public ReadFileToDatabase(BufferPool bufferPool, ITransactionalStorage storage, Stream inputStream, string filename)
			{
				this.bufferPool = bufferPool;
				this.inputStream = inputStream;
				this.storage = storage;
				this.filename = filename;
				buffer = bufferPool.TakeBuffer(StorageConstants.MaxPageSize);
			    md5Hasher = Encryptor.Current.CreateHash();
			}

			public string FileHash { get; private set; }

			public void Dispose()
			{
				bufferPool.ReturnBuffer(buffer);
			}

			public async Task Execute()
			{
				while (true)
				{
					var totalSizeRead = await inputStream.ReadAsync(buffer);

					TotalSizeRead += totalSizeRead;

					if (totalSizeRead == 0) // nothing left to read
					{
						storage.Batch(accessor => accessor.CompleteFileUpload(filename));
					    FileHash = IOExtensions.GetMD5Hex(md5Hasher.TransformFinalBlock());
						return; // task is done
					}

					ConcurrencyAwareExecutor.Execute(() => storage.Batch(accessor =>
					{
						var hashKey = accessor.InsertPage(buffer, totalSizeRead);
						accessor.AssociatePage(filename, hashKey, pos, totalSizeRead);
					}));

					md5Hasher.TransformBlock(buffer, 0, totalSizeRead);

					pos++;
				}
			}
		}
	}
}
