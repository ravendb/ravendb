using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Streams;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Util;
using Raven.Database.Util.Streams;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class FilesController : RavenFsApiController
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/files")]
        public HttpResponseMessage Get()
		{
            int results;
            var keys = Search.Query(null, null, Paging.Start, Paging.PageSize, out results);

            var list = new List<FileHeader>();
            Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));

            return this.GetMessageWithObject(list, HttpStatusCode.OK);
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Get(string name)
		{
			name = RavenFileNameHelper.RavenPath(name);
			FileAndPages fileAndPages = null;
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
			var result = StreamResult(name, readingStream);
            AddHeaders(result, fileAndPages.Metadata);
			return result;
		}

		[HttpDelete]
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Delete(string name)
		{
			name = RavenFileNameHelper.RavenPath(name);

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

			Publisher.Publish(new FileChange { File = FilePathTools.Cannoicalise(name), Action = FileChangeAction.Delete });
			log.Debug("File '{0}' was deleted", name);

			StartSynchronizeDestinationsInBackground();

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpHead]
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Head(string name)
		{
			name = RavenFileNameHelper.RavenPath(name);
			FileAndPages fileAndPages = null;
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
			AddHeaders(httpResponseMessage, fileAndPages.Metadata);
			return httpResponseMessage;
		}

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Post(string name)
		{
			name = RavenFileNameHelper.RavenPath(name);

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

            Publisher.Publish(new FileChange { File = FilePathTools.Cannoicalise(name), Action = FileChangeAction.Update });

            StartSynchronizeDestinationsInBackground();

            log.Debug("Metadata of a file '{0}' was updated", name);

            //Hack needed by jquery on the client side. We need to find a better solution for this
            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPatch]
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Patch(string name, string rename)
		{
			name = RavenFileNameHelper.RavenPath(name);
			rename = RavenFileNameHelper.RavenPath(rename);

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
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
		public async Task<HttpResponseMessage> Put(string name, string uploadId = null)
		{
			try
			{
                RavenFileSystem.MetricsCounters.FilesPerSecond.Mark();

				name = RavenFileNameHelper.RavenPath(name);

                var headers = this.GetFilteredMetadataFromHeaders(InnerHeaders);

                Historian.UpdateLastModified(headers);
                Historian.Update(name, headers);

                SynchronizationTask.Cancel(name);

                ConcurrencyAwareExecutor.Execute(() => Storage.Batch(accessor =>
                {
                    AssertFileIsNotBeingSynced(name, accessor, true);
                    StorageOperationsTask.IndicateFileToDelete(name);

                    var contentLength = Request.Content.Headers.ContentLength;
                    var sizeHeader = GetHeader("RavenFS-size");
                    long? size;
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

                log.Debug("Inserted a new file '{0}' with ETag {1}", name, headers.Value<Guid>("ETag"));

                using (var contentStream = await Request.Content.ReadAsStreamAsync())
                using (var readFileToDatabase = new ReadFileToDatabase(BufferPool, Storage, contentStream, name))
                {
                    await readFileToDatabase.Execute();

                    Historian.UpdateLastModified(headers); // update with the final file size

                    log.Debug("File '{0}' was uploaded. Starting to update file metadata and indexes", name);

                    headers["Content-MD5"] = readFileToDatabase.FileHash;

                    Storage.Batch(accessor => accessor.UpdateFileMetadata(name, headers));
                    headers["Content-Length"] = readFileToDatabase.TotalSizeRead.ToString(CultureInfo.InvariantCulture);
                    Search.Index(name, headers);
                    Publisher.Publish(new FileChange { Action = FileChangeAction.Add, File = FilePathTools.Cannoicalise(name) });

                    log.Debug("Updates of '{0}' metadata and indexes were finished. New file ETag is {1}", name, headers.Value<Guid>("ETag"));

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
						Publisher.Publish(new UploadFailed { UploadId = uploadIdentifier, File = name });
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
			Task.Factory.StartNew(async () => await SynchronizationTask.SynchronizeDestinationsAsync(), CancellationToken.None,
								  TaskCreationOptions.None, TaskScheduler.Default);
		}

        private static void AddHeaders(HttpResponseMessage context, RavenJObject metadata)
        {
            foreach (var item in metadata)
            {
                if (item.Key == "ETag")
                {
                    var etag = item.Value.Value<Guid>();
                    if (etag == null)
                        continue;

                    context.Headers.ETag = new EntityTagHeaderValue(@"""" + etag + @"""");
                }
                else
                {
                    if (item.Key == "Last-Modified")
                    {
                        string value = item.Value.Value<string>();
                        context.Content.Headers.Add(item.Key, new Regex("\\.\\d{5}").Replace(value, string.Empty)); // HTTP does not provide milliseconds, so remove it
                    }
                    else
                    {
                        string value;
                        switch (item.Value.Type)
                        {
                            // REVIEW: Can we just do item.Value.ToString(Imports.Newtonsoft.Json.Formatting.None) everywhere?
                            case JTokenType.Object:
                            case JTokenType.Array:
                                value = item.Value.ToString(Imports.Newtonsoft.Json.Formatting.None);
                                break;
                            default:
                                value = item.Value.Value<string>();
                                break;
                        }
                        context.Content.Headers.Add(item.Key, value);
                    }
                }
            }
        }

		private class ReadFileToDatabase : IDisposable
		{
			private readonly byte[] buffer;
			private readonly BufferPool bufferPool;
			private readonly string filename;
			private readonly Stream inputStream;
			private readonly MD5 md5Hasher;
			private readonly ITransactionalStorage storage;
			public int TotalSizeRead;
			private int pos;

			public ReadFileToDatabase(BufferPool bufferPool, ITransactionalStorage storage, Stream inputStream, string filename)
			{
				this.bufferPool = bufferPool;
				this.inputStream = inputStream;
				this.storage = storage;
				this.filename = filename;
				buffer = bufferPool.TakeBuffer(StorageConstants.MaxPageSize);
				md5Hasher = new MD5CryptoServiceProvider();
			}

			public string FileHash { get; private set; }

			public void Dispose()
			{
				bufferPool.ReturnBuffer(buffer);
				md5Hasher.Dispose();
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
						md5Hasher.TransformFinalBlock(new byte[0], 0, 0);

						FileHash = md5Hasher.Hash.ToStringHash();

						return; // task is done
					}

					ConcurrencyAwareExecutor.Execute(() => storage.Batch(accessor =>
					{
						var hashKey = accessor.InsertPage(buffer, totalSizeRead);
						accessor.AssociatePage(filename, hashKey, pos, totalSizeRead);
					}));

					md5Hasher.TransformBlock(buffer, 0, totalSizeRead, null, 0);

					pos++;
				}
			}
		}
	}
}
