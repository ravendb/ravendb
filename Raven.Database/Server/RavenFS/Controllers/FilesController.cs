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

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class FilesController : RavenFsApiController
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/files")]
		public List<FileHeader> Get()
		{
            int results;
            var keys = Search.Query(null, null, Paging.Start, Paging.PageSize, out results);

            var list = new List<FileHeader>();
            Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));

            return list;
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

			if (fileAndPages.Metadata.AllKeys.Contains(SynchronizationConstants.RavenDeleteMarker))
			{
				log.Debug("File '{0}' is not accessible to get (Raven-Delete-Marker set)", name);
				throw new HttpResponseException(HttpStatusCode.NotFound);
			}

			var readingStream = StorageStream.Reading(Storage, name);
			var result = StreamResult(name, readingStream);
			MetadataExtensions.AddHeaders(result, fileAndPages);
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

					if (
						metadata.AllKeys.Contains(
							SynchronizationConstants.RavenDeleteMarker))
					{
						throw new FileNotFoundException();
					}

					StorageOperationsTask.IndicateFileToDelete(name);

					if (
						!name.EndsWith(RavenFileNameHelper.DownloadingFileSuffix) &&
						// don't create a tombstone for .downloading file
						metadata != null) // and if file didn't exist
					{
						var tombstoneMetadata = new NameValueCollection
						{
							{
								SynchronizationConstants
									.RavenSynchronizationHistory,
								metadata[
									SynchronizationConstants
										.RavenSynchronizationHistory]
							},
							{
								SynchronizationConstants
									.RavenSynchronizationVersion,
								metadata[
									SynchronizationConstants
										.RavenSynchronizationVersion]
							},
							{
								SynchronizationConstants
									.RavenSynchronizationSource,
								metadata[
									SynchronizationConstants
										.RavenSynchronizationSource]
							}
						}.WithDeleteMarker();

						Historian.UpdateLastModified(tombstoneMetadata);
						accessor.PutFile(name, 0, tombstoneMetadata, true);

						accessor.DeleteConfig(
							RavenFileNameHelper.ConflictConfigNameForFile(name));
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

			return new HttpResponseMessage(HttpStatusCode.NoContent);
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

			if (fileAndPages.Metadata.AllKeys.Contains(SynchronizationConstants.RavenDeleteMarker))
			{
				log.Debug("Cannot get metadata of a file '{0}' because file was deleted", name);
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			}

			var httpResponseMessage = GetEmptyMessage();
			MetadataExtensions.AddHeaders(httpResponseMessage, fileAndPages);
			return httpResponseMessage;
		}

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Post(string name)
		{
			name = RavenFileNameHelper.RavenPath(name);

			var headers = Request.Headers.FilterHeaders();
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
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			}

			Search.Index(name, headers);

			Publisher.Publish(new FileChange { File = FilePathTools.Cannoicalise(name), Action = FileChangeAction.Update });

			StartSynchronizeDestinationsInBackground();

			log.Debug("Metadata of a file '{0}' was updated", name);
			return new HttpResponseMessage(HttpStatusCode.NoContent);
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

													 if (
														 metadata.AllKeys.Contains(
															 SynchronizationConstants.RavenDeleteMarker))
													 {
														 throw new FileNotFoundException();
													 }

													 var existingHeader = accessor.ReadFile(rename);
													 if (existingHeader != null &&
														 !existingHeader.Metadata.AllKeys.Contains(
															 SynchronizationConstants.RavenDeleteMarker))
													 {
														 throw new HttpResponseException(
															 Request.CreateResponse(HttpStatusCode.Forbidden,
																					new InvalidOperationException(
																						"Cannot rename because file " + rename +
																						" already exists")));
													 }

													 Historian.UpdateLastModified(metadata);

													 var operation = new RenameFileOperation
													 {
														 Name = name,
														 Rename = rename,
														 MetadataAfterOperation = metadata
													 };

													 accessor.SetConfig(
														 RavenFileNameHelper.RenameOperationConfigNameForFile(name),
														 operation.AsConfig());
													 accessor.PulseTransaction(); // commit rename operation config

													 StorageOperationsTask.RenameFile(operation);
												 }), ConcurrencyResponseException);
			}
			catch (FileNotFoundException)
			{
				log.Debug("Cannot rename a file '{0}' to '{1}' because a file was not found", name, rename);
				return new HttpResponseMessage(HttpStatusCode.NotFound);
			}

			log.Debug("File '{0}' was renamed to '{1}'", name, rename);

			StartSynchronizeDestinationsInBackground();

			return new HttpResponseMessage(HttpStatusCode.NoContent);
		}

		[HttpPut]
        [Route("ravenfs/{fileSystemName}/files/{*name}")]
		public async Task<HttpResponseMessage> Put(string name, string uploadId = null)
		{
			try
			{
                RavenFileSystem.MetricsCounters.FilesPerSecond.Mark();

				name = RavenFileNameHelper.RavenPath(name);

				var headers = InnerHeaders.FilterHeaders();
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

					log.Debug("Updates of '{0}' metadata and indexes were finished. New file ETag is {1}", name,
							  headers.Value<Guid>("ETag"));

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

			return new HttpResponseMessage(HttpStatusCode.Created);
		}

		private void StartSynchronizeDestinationsInBackground()
		{
			Task.Factory.StartNew(async () => await SynchronizationTask.SynchronizeDestinationsAsync(), CancellationToken.None,
								  TaskCreationOptions.None, TaskScheduler.Default);
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
