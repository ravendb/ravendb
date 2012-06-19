using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Bundles.Encryption.Settings;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption.Plugin
{
	public class DocumentEncryption : AbstractDocumentCodec
	{
		private EncryptionSettings settings;

		public override void Initialize(DocumentDatabase database)
		{
			settings = EncryptionSettingsManager.GetEncryptionSettingsForDatabase(database);
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
