// -----------------------------------------------------------------------
//  <copyright file="FileEncryption.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.Bundles.Encryption.Settings;
using Raven.Database.FileSystem.Plugins;

namespace Raven.Database.FileSystem.Bundles.Encryption.Plugin
{
	[InheritedExport(typeof(AbstractFileCodec))]
	[ExportMetadata("Order", 5000)]
	[ExportMetadata("Bundle", "Encryption")]
	public class FileEncryption : AbstractFileCodec
	{
		private const string PageEncryptionMarker = "{AE63BE19}";

		private EncryptionSettings settings;

		public override void Initialize()
		{
			settings = EncryptionSettingsManager.GetEncryptionSettingsForResource(FileSystem);
		}

		public override void SecondStageInit()
		{
			EncryptionSettingsManager.VerifyEncryptionKey(FileSystem, settings);
		}

		public override Stream EncodePage(Stream data)
		{
			return settings.Codec.Encode(PageEncryptionMarker, data);
		}

		public override Stream DecodePage(Stream encodedDataStream)
		{
			return settings.Codec.Decode(PageEncryptionMarker, encodedDataStream);
		}
	}
}