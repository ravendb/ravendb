using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Json.Linq;

namespace Raven.Bundles.Encryption
{
	internal class Constants
	{
		public const string DontEncryptDocumentsStartingWith = "Raven/";
		public const string AlgorithmTypeSetting = "Raven/Encryption/Algorithm";
		public const string EncryptionKeySetting = "Raven/Encryption/Key";
		public const string EncryptIndexes = "Raven/Encryption/EncryptIndexes";

		public const string InDatabaseKeyVerificationDocumentName = "Raven/Encryption/Verification";
		public static readonly RavenJObject InDatabaseKeyVerificationDocumentContents = new RavenJObject {
			{ "Text", "The encryption is correct." }
		};

		public const int DefaultGeneratedEncryptionKeyLength = 256 / 8;
		public const int MinimumAcceptableEncryptionKeyLength = 64 / 8;

		public const int DefaultKeySizeToUseInActualEncryptionInBits = 128;
		public const int Rfc2898Iterations = 1000;

		public const int DefaultIndexFileBlockSize = 12 * 1024;

		public static readonly Type DefaultCryptoServiceProvider = typeof(AesManaged);

	}
}
