using System;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;
using Raven.Abstractions;
using X509Certificate = Org.BouncyCastle.X509.X509Certificate;

namespace Raven.Database.Config
{
	public static class CertGenerator
	{
		public static void GenerateNewCertificate(string name, Stream stream)
		{
			var kpGen = new RsaKeyPairGenerator();
			kpGen.Init(new KeyGenerationParameters(new SecureRandom(new CryptoApiRandomGenerator()), 1024));

			AsymmetricCipherKeyPair keyPair = kpGen.GenerateKeyPair();
			var gen = new X509V3CertificateGenerator();
			var certificateName = new X509Name("CN=" + name);
			BigInteger serialNumber = BigInteger.ProbablePrime(120, new Random());
			gen.SetSerialNumber(serialNumber);
			gen.SetSubjectDN(certificateName);
			gen.SetIssuerDN(certificateName);
			gen.SetNotAfter(SystemTime.UtcNow.AddYears(100));
			gen.SetNotBefore(SystemTime.UtcNow.AddDays(-1));
			gen.SetSignatureAlgorithm("SHA256WithRSAEncryption");
			gen.SetPublicKey(keyPair.Public);

			gen.AddExtension(X509Extensions.AuthorityKeyIdentifier.Id, false,
							 new AuthorityKeyIdentifier(
								SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(keyPair.Public),
								new GeneralNames(new GeneralName(certificateName)), serialNumber));

			X509Certificate newCert = gen.Generate(keyPair.Private);

			var newStore = new Pkcs12Store();

			var certEntry = new X509CertificateEntry(newCert);

			newStore.SetCertificateEntry(
				Environment.MachineName,
				certEntry
				);

			newStore.SetKeyEntry(
			Environment.MachineName,
			new AsymmetricKeyEntry(keyPair.Private),
			new[] { certEntry }
			);

			newStore.Save(
				stream,
				new char[0],
				new SecureRandom(new CryptoApiRandomGenerator())
				);

		}
		
		private static readonly ConcurrentDictionary<string, Lazy<X509Certificate2>> cache = new ConcurrentDictionary<string, Lazy<X509Certificate2>>();

		public static X509Certificate2 GenerateNewCertificate(string name)
		{
			var lazy = cache.GetOrAdd(name, s => new Lazy<X509Certificate2>(() =>
			{
				var memoryStream = new MemoryStream();

				GenerateNewCertificate(name, memoryStream);

				try
				{
					return new X509Certificate2(memoryStream.ToArray(), string.Empty, X509KeyStorageFlags.MachineKeySet);
				}
				catch (Exception)
				{
					return new X509Certificate2(memoryStream.ToArray(), string.Empty, X509KeyStorageFlags.DefaultKeySet);
				}
			}));
			return lazy.Value;
		}
	}
}