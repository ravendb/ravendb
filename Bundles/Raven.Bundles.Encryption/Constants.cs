using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Bundles.Encryption
{
	internal class Constants
	{
		public const string DontEncryptDocumentsStartingWith = "Raven/";
		public const string EncryptionSettingsDocumentKey = "Raven/Encryption/Settings";

		public const int DefaultGeneratedEncryptionKeyLength = 256 / 8;
		public const int MinimumAcceptableEncryptionKeyLength = 64 / 8;

		public const int DefaultKeySizeToUseInActualEncryptionInBits = 128;
		public const int Rfc2898Iterations = 1000;

		public static readonly Type DefaultCryptoServiceProvider = typeof(AesManaged);

	}
}
