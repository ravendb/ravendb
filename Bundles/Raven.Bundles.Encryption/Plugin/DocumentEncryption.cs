using System.ComponentModel.Composition;
using System.IO;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption.Plugin
{
	[InheritedExport(typeof(AbstractDocumentCodec))]
	[ExportMetadata("Order", 10000)]
	public class DocumentEncryption : AbstractDocumentCodec
	{
		public override Stream Encode(string key, RavenJObject data, RavenJObject metadata, Stream dataStream)
		{
			if (EncryptionSettings.DontEncrypt(key))
				return dataStream;

			return Codec.Encode(key, dataStream);
		}

		public override Stream Decode(string key, RavenJObject metadata, Stream dataStream)
		{
			if (EncryptionSettings.DontEncrypt(key))
				return dataStream;

			return Codec.Decode(key, dataStream);
		}
	}
}
