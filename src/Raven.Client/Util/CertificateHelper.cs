#nullable enable
#if NET9_0_OR_GREATER2
#define FEATURE_X509CERTIFICATELOADER_SUPPORT
#endif
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;

namespace Raven.Client.Util;

internal static class CertificateHelper
{
    public static X509Certificate2 CreateCertificate(byte[] rawData)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        var certificate = X509CertificateLoader.LoadCertificate(rawData);
#else
        var certificate = new X509Certificate2(rawData);
#endif

        AssertCertificate(certificate);
        return certificate;
    }

    public static X509Certificate2 CreateCertificate(string fileName)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        var certificate = X509CertificateLoader.LoadCertificateFromFile(fileName);
#else
        var certificate = new X509Certificate2(fileName);
#endif

        AssertCertificate(certificate);
        return certificate;
    }

    public static X509Certificate2 CreateCertificate(byte[] rawData, string? password, X509KeyStorageFlags keyStorageFlags)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        var certificate = X509CertificateLoader.LoadPkcs12(rawData, password, keyStorageFlags);
#else
        var certificate = new X509Certificate2(rawData, password, keyStorageFlags);
#endif

        AssertCertificate(certificate);
        return certificate;
    }

    public static X509Certificate2 CreateCertificate(string path, string? password, X509KeyStorageFlags keyStorageFlags)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        var certificate = X509CertificateLoader.LoadPkcs12FromFile(path, password, keyStorageFlags);
#else
        var certificate = new X509Certificate2(path, password, keyStorageFlags);
#endif

        AssertCertificate(certificate);
        return certificate;
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
