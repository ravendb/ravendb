#nullable enable
#if NET9_0_OR_GREATER
#define FEATURE_X509CERTIFICATELOADER_SUPPORT
#endif
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Raven.Client.Util;

internal static class CertificateHelper
{
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
    private static readonly Pkcs12LoaderLimits AllowDuplicateAttributes;

    static CertificateHelper()
    {
        AllowDuplicateAttributes = new Pkcs12LoaderLimits(Pkcs12LoaderLimits.Defaults);

        var allowDuplicateAttributesProperty = AllowDuplicateAttributes.GetType().GetProperty(nameof(AllowDuplicateAttributes), BindingFlags.Instance | BindingFlags.NonPublic);
        Debug.Assert(allowDuplicateAttributesProperty != null, $"{nameof(allowDuplicateAttributesProperty)} != null");
        allowDuplicateAttributesProperty.SetValue(AllowDuplicateAttributes, true);
    }


    public static RSAParameters GetRsaParametersSafely(this RSA rsa)
    {
        // workaround for https://github.com/dotnet/runtime/issues/109059#issuecomment-2426833907
        using (RSA tempRsa = RSA.Create())
        {
            var tempPassword = "TempPassword";
            var pbeParameters = new PbeParameters(PbeEncryptionAlgorithm.Aes128Cbc, HashAlgorithmName.SHA256, 1);
            tempRsa.ImportEncryptedPkcs8PrivateKey(tempPassword, rsa.ExportEncryptedPkcs8PrivateKey(tempPassword, pbeParameters), out _);
            return tempRsa.ExportParameters(true);
        }
    }

    public static RSAParameters GetRsaParametersSafely(this X509Certificate2 certificate)
    {
        var rsa = certificate.GetRSAPrivateKey();
        if (rsa == null)
            throw new InvalidOperationException("Could not extract RSA Private Key from the certificate.");

        return rsa.GetRsaParametersSafely();
    }
#endif

    public static X509Certificate2 CreateCertificateFromCert(byte[] rawData)
    {
        ValidateContentType(rawData, X509ContentType.Cert);

#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        var certificate = X509CertificateLoader.LoadCertificate(rawData);
#else
        var certificate = new X509Certificate2(rawData);
#endif

        AssertCertificate(certificate);
        return certificate;
    }

    public static X509Certificate2 CreateCertificateFromCert(string fileName)
    {
        ValidateContentType(fileName, X509ContentType.Cert);

#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        var certificate = X509CertificateLoader.LoadCertificateFromFile(fileName);
#else
        var certificate = new X509Certificate2(fileName);
#endif

        AssertCertificate(certificate);
        return certificate;
    }

    public static X509Certificate2 CreateCertificateFromPfx(byte[] rawData, string? password, X509KeyStorageFlags keyStorageFlags)
    {
        ValidateContentType(rawData, X509ContentType.Pfx);

#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        var certificate = X509CertificateLoader.LoadPkcs12(rawData, password, keyStorageFlags, AllowDuplicateAttributes);
#else
        var certificate = new X509Certificate2(rawData, password, keyStorageFlags);
#endif

        AssertCertificate(certificate);
        return certificate;
    }

    public static X509Certificate2 CreateCertificateFromPfx(string path, string? password, X509KeyStorageFlags keyStorageFlags)
    {
        var certBytes = File.ReadAllBytes(path);
        return CreateCertificateFromPfx(certBytes, password, keyStorageFlags);
    }

    [Conditional("DEBUG")]
    private static void ValidateContentType(byte[] rawData, X509ContentType expectedContentType)
    {
        var contentType = X509Certificate2.GetCertContentType(rawData);
        if (contentType != expectedContentType)
            throw new InvalidOperationException($"Was expecting '{expectedContentType}' but got '{contentType}'.");
    }

    [Conditional("DEBUG")]
    private static void ValidateContentType(string fileName, X509ContentType expectedContentType)
    {
        var contentType = X509Certificate2.GetCertContentType(fileName);
        if (contentType != expectedContentType)
            throw new InvalidOperationException($"Was expecting '{expectedContentType}' but got '{contentType}'.");
    }

    [Conditional("DEBUG")]
    private static void AssertCertificate(X509Certificate2 certificate)
    {
        if (certificate.HasPrivateKey)
        {
            _ = certificate.GetRSAPrivateKey();
        }
    }
}
