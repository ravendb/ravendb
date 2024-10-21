using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;
using Raven.Client.Util;
using X509Certificate = Org.BouncyCastle.X509.X509Certificate;

namespace Tests.Infrastructure.Utils;

public static class CertificateGenerator
{
    public static X509Certificate2 GenerateRootCACertificate(string subjectName, int yearsValid, AsymmetricCipherKeyPair keyPair)
    {
        return GenerateCertificate(subjectName, yearsValid, keyPair, isCa: true);
    }

    public static AsymmetricCipherKeyPair GenerateRSAKeyPair()
    {
        var rsaGenerator = new RsaKeyPairGenerator();
        rsaGenerator.Init(new KeyGenerationParameters(new SecureRandom(), 2048));
        return rsaGenerator.GenerateKeyPair();
    }

    public static X509Certificate2 GenerateIntermediateCACertificate(X509Certificate2 rootCertificate, AsymmetricCipherKeyPair rootPrivateKey, string subjectName,
        int yearsValid, AsymmetricCipherKeyPair intermediateKeyPair)
    {
        return GenerateCertificate(subjectName, yearsValid, intermediateKeyPair, signerKeyPair: rootPrivateKey, signerCertificate: rootCertificate, isCa: true);
    }

    public static X509Certificate2 GenerateSignedClientCertificate(X509Certificate2 signerCertificate, AsymmetricCipherKeyPair signerKeyPair, string subjectName,
        int yearsValid, AsymmetricCipherKeyPair clientKeyPair, string[] sans = null)
    {
        var keyUsage = new KeyUsage(KeyUsage.DigitalSignature | KeyUsage.KeyEncipherment);
        var extendedKeyUsage = new ExtendedKeyUsage(new[] { KeyPurposeID.id_kp_serverAuth, KeyPurposeID.id_kp_clientAuth });
        GeneralNames subjectAlternativeNames;
        if (sans == null)
        {
            subjectAlternativeNames = new GeneralNames(new GeneralName(GeneralName.DnsName, "localhost"));
        }
        else if (sans.Length == 0)
        {
            subjectAlternativeNames = null;
        }
        else
        {
            var names = new List<GeneralName>(sans.Length);
            foreach (var san in sans)
            {
                names.Add(new GeneralName(GeneralName.DnsName, san));
            }

            subjectAlternativeNames = new GeneralNames(names.ToArray());
    }

        return GenerateCertificate(subjectName, yearsValid, clientKeyPair, keyUsage, extendedKeyUsage, subjectAlternativeNames, signerCertificate, signerKeyPair);
    }

    public static X509Certificate2 GenerateSelfSignedClientCertificate(string subjectName, int yearsValid, AsymmetricCipherKeyPair keyPair)
    {
        var keyUsage = new KeyUsage(KeyUsage.DigitalSignature | KeyUsage.KeyEncipherment);
        var extendedKeyUsage = new ExtendedKeyUsage(KeyPurposeID.id_kp_serverAuth, KeyPurposeID.id_kp_clientAuth);
        var subjectAlternativeName = new GeneralNames(new GeneralName(GeneralName.DnsName, "localhost"));

        return GenerateCertificate(subjectName, yearsValid, keyPair, keyUsage, extendedKeyUsage, subjectAlternativeName, isCa: true);
    }

    private static X509Certificate2 GenerateCertificate(string subjectName, int yearsValid, AsymmetricCipherKeyPair keyPair, KeyUsage keyUsage = null,
        ExtendedKeyUsage extendedKeyUsage = null, GeneralNames subjectAlternativeName = null, X509Certificate2 signerCertificate = null,
        AsymmetricCipherKeyPair signerKeyPair = null, bool isCa = false)
    {
        var certGenerator = new X509V3CertificateGenerator();
        certGenerator.SetSerialNumber(BigInteger.ProbablePrime(120, new Random()));
        certGenerator.SetSubjectDN(new X509Name($"CN={subjectName}"));
        certGenerator.SetIssuerDN(signerCertificate != null ? new X509Name(signerCertificate.Subject) : new X509Name($"CN={subjectName}"));
        certGenerator.SetNotBefore(DateTime.UtcNow);
        certGenerator.SetNotAfter(DateTime.UtcNow.AddYears(yearsValid));
        certGenerator.SetPublicKey(keyPair.Public);

        if (isCa)
        {
            certGenerator.AddExtension(
                X509Extensions.BasicConstraints.Id,
                true,
                new BasicConstraints(true));
        }

        if (keyUsage != null)
        {
            certGenerator.AddExtension(
                X509Extensions.KeyUsage.Id,
                true,
                keyUsage);
        }

        if (extendedKeyUsage != null)
        {
            certGenerator.AddExtension(
                X509Extensions.ExtendedKeyUsage.Id,
                true,
                extendedKeyUsage);
        }

        if (subjectAlternativeName != null)
        {
            certGenerator.AddExtension(
                X509Extensions.SubjectAlternativeName.Id,
                false,
                subjectAlternativeName);
        }

        var signatureFactory = new Asn1SignatureFactory("SHA256WITHRSA", signerKeyPair != null ? signerKeyPair.Private : keyPair.Private);
        var certificate = certGenerator.Generate(signatureFactory);

        return IncludePrivateKeyWithTheCertificate(certificate, keyPair);
    }

    private static X509Certificate2 IncludePrivateKeyWithTheCertificate(X509Certificate certificate, AsymmetricCipherKeyPair keyPair)
    {
        var random = new SecureRandom();
        var store = new Pkcs12StoreBuilder().Build();
        string friendlyName = certificate.SubjectDN.ToString();
        var certificateEntry = new X509CertificateEntry(certificate);
        var keyEntry = new AsymmetricKeyEntry(keyPair.Private);

        store.SetCertificateEntry(friendlyName, certificateEntry);
        store.SetKeyEntry(friendlyName, keyEntry, new[] { certificateEntry });
        var stream = new MemoryStream();
        store.Save(stream, Array.Empty<char>(), random);

        return CertificateLoaderUtil.CreateCertificateFromAny(stream.ToArray());
}
}
