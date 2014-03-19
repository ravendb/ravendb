using System.Collections.Specialized;
using System.Security.Cryptography;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Infrastructure;
using Raven.Database.Server.RavenFS.Search;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;

namespace Raven.Database.Server.RavenFS.Util
{
	public class SynchronizingFileStream : StorageStream
	{
		private readonly MD5 md5Hasher;

		private SynchronizingFileStream(ITransactionalStorage transactionalStorage, string fileName,
										StorageStreamAccess storageStreamAccess, NameValueCollection metadata,
										IndexStorage indexStorage, StorageOperationsTask operations)
			: base(transactionalStorage, fileName, storageStreamAccess, metadata, indexStorage, operations)
		{
			md5Hasher = new MD5CryptoServiceProvider();
		}

		public bool PreventUploadComplete { get; set; }

		public string FileHash { get; private set; }

		public override void Flush()
		{
			if (InnerBuffer != null && InnerBufferOffset > 0)
			{
				md5Hasher.TransformBlock(InnerBuffer, 0, InnerBufferOffset, null, 0);
				base.Flush();
			}
		}

		protected override void Dispose(bool disposing)
		{
			if (!PreventUploadComplete)
			{
				base.Dispose(disposing);

				md5Hasher.TransformFinalBlock(new byte[0], 0, 0);
				FileHash = md5Hasher.Hash.ToStringHash();
				md5Hasher.Dispose();
			}
		}

		public static SynchronizingFileStream CreatingOrOpeningAndWritting(ITransactionalStorage storage, IndexStorage search,
																		   StorageOperationsTask operationsTask,
																		   string fileName, NameValueCollection metadata)
		{
			return new SynchronizingFileStream(storage, fileName, StorageStreamAccess.CreateAndWrite, metadata, search,
											   operationsTask) { PreventUploadComplete = true };
		}
	}
}