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

namespace Raven.Bundles.Encryption
{
	public class DocumentEncryption : AbstractDocumentCodec
	{
		private static EncryptionSettings EncryptionSettings
		{
			get { return Codec.EncryptionSettings; }
		}

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
