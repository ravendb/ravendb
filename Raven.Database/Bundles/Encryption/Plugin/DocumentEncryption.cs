using System.ComponentModel.Composition;
using System.IO;
using Raven.Bundles.Encryption.Settings;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption.Plugin
{
	[InheritedExport(typeof(AbstractDocumentCodec))]
	[ExportMetadata("Order", 10000)]
	[ExportMetadata("Bundle", "Encryption")]
	public class DocumentEncryption : AbstractDocumentCodec
	{
		private EncryptionSettings settings;

		public override void Initialize(DocumentDatabase database)
		{
			settings = EncryptionSettingsManager.GetEncryptionSettingsForDatabase(database);


			EncryptionSettingsManager.VerifyEncryptionKey(database, settings);
		}

		public override Stream Encode(string key, RavenJObject data, RavenJObject metadata, Stream dataStream)
		{
			if (EncryptionSettings.DontEncrypt(key))
				return dataStream;

			return settings.Codec.Encode(key, dataStream);
		}

		public override Stream Decode(string key, RavenJObject metadata, Stream dataStream)
		{
			if (EncryptionSettings.DontEncrypt(key))
				return dataStream;

			return settings.Codec.Decode(key, dataStream);
		}
	}
}
