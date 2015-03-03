// -----------------------------------------------------------------------
//  <copyright file="FileActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Actions
{
	public class FileActions : ActionsBase
	{
		public FileActions(RavenFileSystem fileSystem, ILog log)
			: base(fileSystem, log)
		{
		}

		public async Task PutAsync(string name, RavenJObject metadata, Func<Task<Stream>> streamAsync, PutOperationOptions options)
		{
			try
			{
				FileSystem.MetricsCounters.FilesPerSecond.Mark();

				name = FileHeader.Canonize(name);

				if (options.PreserveTimestamps)
				{
					if (!metadata.ContainsKey(Constants.RavenCreationDate))
					{
						if (metadata.ContainsKey(Constants.CreationDate))
							metadata[Constants.RavenCreationDate] = metadata[Constants.CreationDate];
						else
							throw new InvalidOperationException("Preserve Timestamps requires that the client includes the Raven-Creation-Date header.");
					}

					Historian.UpdateLastModified(metadata, options.LastModified.HasValue ? options.LastModified.Value : DateTimeOffset.UtcNow);
				}
				else
				{
					metadata[Constants.RavenCreationDate] = DateTimeOffset.UtcNow;

					Historian.UpdateLastModified(metadata);
				}

				// TODO: To keep current filesystems working. We should remove when adding a new migration. 
				metadata[Constants.CreationDate] = metadata[Constants.RavenCreationDate].Value<DateTimeOffset>().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);

				Historian.Update(name, metadata);

				SynchronizationTask.Cancel(name);

				long? size = -1;
				Storage.Batch(accessor =>
				{
					AssertPutOperationNotVetoed(name, metadata);
					AssertFileIsNotBeingSynced(name, accessor);

					var contentLength = options.ContentLength;
					var contentSize = options.ContentSize;

					if (contentLength == 0 || contentSize.HasValue == false)
					{
						size = contentLength;
						if (options.TransferEncodingChunked)
							size = null;
					}
					else
					{
						size = contentSize;
					}

					FileSystem.PutTriggers.Apply(trigger => trigger.OnPut(name, metadata));

					using (FileSystem.DisableAllTriggersForCurrentThread())
					{
						StorageOperationsTask.IndicateFileToDelete(name);
					}

					var putResult = accessor.PutFile(name, size, metadata);

					FileSystem.PutTriggers.Apply(trigger => trigger.AfterPut(name, size, metadata));

					Search.Index(name, metadata, putResult.Etag);
				});

				Log.Debug("Inserted a new file '{0}' with ETag {1}", name, metadata.Value<string>(Constants.MetadataEtagField));

				using (var contentStream = await streamAsync())
				using (var readFileToDatabase = new ReadFileToDatabase(BufferPool, Storage, FileSystem.PutTriggers, contentStream, name, metadata))
				{
					await readFileToDatabase.Execute();

					if (readFileToDatabase.TotalSizeRead != size)
					{
						StorageOperationsTask.IndicateFileToDelete(name);
						throw new HttpResponseException(HttpStatusCode.BadRequest);
					}

					if (options.PreserveTimestamps == false)
						Historian.UpdateLastModified(metadata); // update with the final file size.

					Log.Debug("File '{0}' was uploaded. Starting to update file metadata and indexes", name);

					metadata["Content-MD5"] = readFileToDatabase.FileHash;

					FileOperationResult updateMetadata = null;
					Storage.Batch(accessor => updateMetadata = accessor.UpdateFileMetadata(name, metadata, null)); //TODO arek

					int totalSizeRead = readFileToDatabase.TotalSizeRead;
					metadata["Content-Length"] = totalSizeRead.ToString(CultureInfo.InvariantCulture);

					Search.Index(name, metadata, updateMetadata.Etag);
					Publisher.Publish(new FileChangeNotification { Action = FileChangeAction.Add, File = FilePathTools.Cannoicalise(name) });

					Log.Debug("Updates of '{0}' metadata and indexes were finished. New file ETag is {1}", name, metadata.Value<string>(Constants.MetadataEtagField));

					FileSystem.Synchronization.StartSynchronizeDestinationsInBackground();
				}
			}
			catch (Exception ex)
			{
				if (options.UploadId.HasValue)
					Publisher.Publish(new CancellationNotification { UploadId = options.UploadId.Value, File = name });

				Log.WarnException(string.Format("Failed to upload a file '{0}'", name), ex);

				throw;
			}
		}

		private void AssertPutOperationNotVetoed(string name, RavenJObject headers)
		{
			var vetoResult = FileSystem.PutTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(name, headers) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("PUT vetoed on file " + name + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertFileIsNotBeingSynced(string fileName, IStorageActionsAccessor accessor)
		{
			if (FileLockManager.TimeoutExceeded(fileName, accessor))
			{
				FileLockManager.UnlockByDeletingSyncConfiguration(fileName, accessor);
			}
			else
			{
				Log.Debug("Cannot execute operation because file '{0}' is being synced", fileName);

				throw new SynchronizationException(string.Format("File {0} is being synced", fileName));
			}
		}

		private class ReadFileToDatabase : IDisposable
		{
			private readonly byte[] buffer;
			private readonly BufferPool bufferPool;
			private readonly string filename;

			private readonly RavenJObject headers;

			private readonly Stream inputStream;
			private readonly ITransactionalStorage storage;

			private readonly OrderedPartCollection<AbstractFilePutTrigger> putTriggers;

			private readonly IHashEncryptor md5Hasher;
			public int TotalSizeRead;
			private int pos;

			public ReadFileToDatabase(BufferPool bufferPool, ITransactionalStorage storage, OrderedPartCollection<AbstractFilePutTrigger> putTriggers, Stream inputStream, string filename, RavenJObject headers)
			{
				this.bufferPool = bufferPool;
				this.inputStream = inputStream;
				this.storage = storage;
				this.putTriggers = putTriggers;
				this.filename = filename;
				this.headers = headers;
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
					var read = await inputStream.ReadAsync(buffer);

					TotalSizeRead += read;

					if (read == 0) // nothing left to read
					{
						FileHash = IOExtensions.GetMD5Hex(md5Hasher.TransformFinalBlock());
						headers["Content-MD5"] = FileHash;
						storage.Batch(accessor =>
						{
							accessor.CompleteFileUpload(filename);
							putTriggers.Apply(trigger => trigger.AfterUpload(filename, headers));
						});
						return; // task is done
					}

					int retries = 50;
					bool shouldRetry;

					do
					{
						try
						{
							storage.Batch(accessor =>
							{
								var hashKey = accessor.InsertPage(buffer, read);
								accessor.AssociatePage(filename, hashKey, pos, read);
								putTriggers.Apply(trigger => trigger.OnUpload(filename, headers, hashKey, pos, read));
							});

							shouldRetry = false;
						}
						catch (ConcurrencyException)
						{
							if (retries-- > 0)
							{
								shouldRetry = true;
								Thread.Sleep(50);
								continue;
							}

							throw;
						}
					} while (shouldRetry);

					md5Hasher.TransformBlock(buffer, 0, read);

					pos++;
				}
			}
		}

		public class PutOperationOptions
		{
			public Guid? UploadId { get; set; }
			
			public bool PreserveTimestamps { get; set; }

			public DateTimeOffset? LastModified { get; set; }

			public long? ContentLength { get; set; }

			public long? ContentSize { get; set; }

			public bool TransferEncodingChunked { get; set; }
		}
	}
}