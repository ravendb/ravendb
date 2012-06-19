using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Bundles.Encryption.Streams;
using Raven.Bundles.Encryption.Settings;
using Raven.Database;

namespace Raven.Bundles.Encryption.Plugin
{
	public class IndexEncryption : AbstractIndexCodec
	{
		public override void Initialize(DocumentDatabase database)
		{
			EncryptionSettingsManager.Initialize(database);
		}

		public override Stream Encode(string key, Stream dataStream)
		{
			// Can't simply use Codec.Encode(key, dataStream) because the resulting stream needs to be seekable

			if (!Codec.EncryptionSettings.EncryptIndexes)
				return dataStream;

			return new SeekableCryptoStream(key, dataStream);
		}

		public override Stream Decode(string key, Stream dataStream)
		{
			// Can't simply use Codec.Decode(key, dataStream) because the resulting stream needs to be seekable

			if (!Codec.EncryptionSettings.EncryptIndexes)
				return dataStream;

			return new SeekableCryptoStream(key, dataStream);
		}
	}
}
