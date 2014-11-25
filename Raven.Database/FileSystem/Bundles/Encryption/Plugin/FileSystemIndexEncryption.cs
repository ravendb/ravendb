// -----------------------------------------------------------------------
//  <copyright file="FileSystemIndexEncryption.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Bundles.Encryption.Settings;
using Raven.Bundles.Encryption.Streams;
using Raven.Database.FileSystem.Plugins;

namespace Raven.Database.FileSystem.Bundles.Encryption.Plugin
{
	[InheritedExport(typeof(AbstractFileSystemIndexCodec))]
	[ExportMetadata("Bundle", "Encryption")]
	[ExportMetadata("Order", 10000)]
	public class FileSystemIndexEncryption : AbstractFileSystemIndexCodec
	{
		EncryptionSettings settings;

		public override void Initialize(RavenFileSystem fileSystem)
		{
			settings = EncryptionSettingsManager.GetEncryptionSettingsForResource(fileSystem);
		}

		public override Stream Encode(string key, Stream dataStream)
		{
			// Can't simply use Codec.Encode(key, dataStream) because the resulting stream needs to be seekable

			if (!settings.EncryptIndexes)
				return dataStream;

			return new SeekableCryptoStream(settings, key, dataStream);
		}

		public override Stream Decode(string key, Stream dataStream)
		{
			// Can't simply use Codec.Decode(key, dataStream) because the resulting stream needs to be seekable

			if (!settings.EncryptIndexes)
				return dataStream;

			return new SeekableCryptoStream(settings, key, dataStream);
		}
	}
}