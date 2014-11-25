// -----------------------------------------------------------------------
//  <copyright file="FileEncryption.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.FileSystem.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Encryption.Plugin
{
	[InheritedExport(typeof(AbstractFileCodec))]
	[ExportMetadata("Order", 5000)]
	[ExportMetadata("Bundle", "Encryption")]
	public class FileEncryption : AbstractFileCodec
	{
		private EncryptionSettings settings;

		public override void Initialize()
		{
			settings = EncryptionSettingsManager.GetEncryptionSettingsForResource(FileSystem);
		}

		public override void SecondStageInit()
		{
			EncryptionSettingsManager.VerifyEncryptionKey(FileSystem, settings);
		}

		public override Stream Encode(string key, Stream data, RavenJObject metadata)
		{
			if (EncryptionSettings.DontEncrypt(key))
				return data;

			return settings.Codec.Encode(key, data);
		}

		public override Stream Decode(string key, Stream encodedDataStream, RavenJObject metadata)
		{
			if (EncryptionSettings.DontEncrypt(key))
				return encodedDataStream;

			return settings.Codec.Decode(key, encodedDataStream);
		}
	}
}