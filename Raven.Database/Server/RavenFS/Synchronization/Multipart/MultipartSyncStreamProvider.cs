using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using Raven.Abstractions.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Synchronization.Multipart
{
	public class MultipartSyncStreamProvider : MultipartStreamProvider
	{
		private readonly StorageStream localFile;
		private readonly SynchronizingFileStream synchronizingFile;

		public MultipartSyncStreamProvider(SynchronizingFileStream synchronizingFile, StorageStream localFile)
		{
			this.synchronizingFile = synchronizingFile;
			this.localFile = localFile;

			BytesTransfered = BytesCopied = NumberOfFileParts = 0;
		}

		public long BytesTransfered { get; private set; }
		public long BytesCopied { get; private set; }
		public long NumberOfFileParts { get; private set; }

		public override Stream GetStream(HttpContent parent, HttpContentHeaders headers)
		{
			if (parent == null)
				throw new ArgumentNullException("parent");

			if (headers == null)
				throw new ArgumentNullException("headers");

			var parameters = headers.ContentDisposition.Parameters.ToDictionary(t => t.Name);

			var needType = parameters[SyncingMultipartConstants.NeedType].Value;
			var from = Convert.ToInt64(parameters[SyncingMultipartConstants.RangeFrom].Value);
			var to = Convert.ToInt64(parameters[SyncingMultipartConstants.RangeTo].Value);
			var length = (to - from + 1);

			NumberOfFileParts++;

			if (needType == "source")
			{
				BytesTransfered += length;

				return synchronizingFile;
			}

			if (needType == "seed")
			{
				var limitedStream = new NarrowedStream(localFile, from, to);
				limitedStream.CopyTo(synchronizingFile, StorageConstants.MaxPageSize);

				BytesCopied += length;

				return Stream.Null; // we can return Stream.Null because 'seed' part is always empty
			}

			throw new ArgumentException(string.Format("Invalid need type: '{0}'", needType));
		}
	}
}