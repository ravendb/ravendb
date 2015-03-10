using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Util
{
	public class SynchronizingFileStream : StorageStream
	{
		private readonly IHashEncryptor md5Hasher;

		private SynchronizingFileStream(RavenFileSystem fileSystem, string fileName,
										RavenJObject metadata, StorageStreamAccess storageStreamAccess)
			: base(fileSystem, fileSystem.Storage, fileName, metadata, storageStreamAccess)
		{
		    md5Hasher = Encryptor.Current.CreateHash();
		}

		public bool PreventUploadComplete { get; set; }

		public string FileHash { get; private set; }

		public override void Flush()
		{
			if (InnerBuffer != null && InnerBufferOffset > 0)
			{
				md5Hasher.TransformBlock(InnerBuffer, 0, InnerBufferOffset);
				base.Flush();
			}
		}

		protected override void Dispose(bool disposing)
		{
			if (!PreventUploadComplete)
			{
				base.Dispose(disposing);

			    FileHash = IOExtensions.GetMD5Hex(md5Hasher.TransformFinalBlock());
				md5Hasher.Dispose();
			}
		}

		public static SynchronizingFileStream CreatingOrOpeningAndWriting(RavenFileSystem fileSystem, string fileName, RavenJObject metadata)
		{
			return new SynchronizingFileStream(fileSystem, fileName, metadata, StorageStreamAccess.CreateAndWrite) { PreventUploadComplete = true };
		}
	}
}