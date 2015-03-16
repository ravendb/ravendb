// -----------------------------------------------------------------------
//  <copyright file="ReadFileToDatabase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Actions
{
	public class ReadFileToDatabase : IDisposable
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
}