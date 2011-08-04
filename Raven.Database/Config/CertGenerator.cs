using System;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;

namespace Raven.Database.Config
{
	public static class CertGenerator
	{
		public static X509Certificate2 GenerateNewCertificate(string name)
		{
			var kpGen = new RsaKeyPairGenerator();
			kpGen.Init(new KeyGenerationParameters(new SecureRandom(new CryptoApiRandomGenerator()), 1024));

			var keyPair = kpGen.GenerateKeyPair();
			var gen = new X509V3CertificateGenerator();
			var certificateName = new X509Name("CN=" + name);
			var serialNumber = BigInteger.ProbablePrime(120, new Random());
			gen.SetSerialNumber(serialNumber);
			gen.SetSubjectDN(certificateName);
			gen.SetIssuerDN(certificateName);
			gen.SetNotAfter(DateTime.Now.AddYears(100));
			gen.SetNotBefore(DateTime.Now.AddDays(-1));
			gen.SetSignatureAlgorithm("SHA1WITHRSA");
			gen.SetPublicKey(keyPair.Public);

			gen.AddExtension(X509Extensions.AuthorityKeyIdentifier.Id, false,
			                 new AuthorityKeyIdentifier(
			                 	SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(keyPair.Public),
			                 	new GeneralNames(new GeneralName(certificateName)), serialNumber));

			var newCertificate = gen.Generate(keyPair.Private);
			var x509Certificate = DotNetUtilities.ToX509Certificate(newCertificate);
			return new X509Certificate2(x509Certificate);
		}
	}
}