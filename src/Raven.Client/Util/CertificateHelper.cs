#nullable enable
using System.Security.Cryptography.X509Certificates;
#if NET9_0_OR_GREATER2
#define FEATURE_X509CERTIFICATELOADER_SUPPORT
#endif

namespace Raven.Client.Util;

internal static class CertificateHelper
{
    public static X509Certificate2 CreateCertificate(byte[] rawData)
    {
        return new X509Certificate2(rawData);
    }

    public static X509Certificate2 CreateCertificate(string fileName)
    {
        return new X509Certificate2(fileName);
    }

    public static X509Certificate2 CreateCertificate(byte[] rawData, string? password, X509KeyStorageFlags keyStorageFlags)
    {
        return new X509Certificate2(rawData, password, keyStorageFlags);
    }

    public static X509Certificate2 CreateCertificate(string path, string? password, X509KeyStorageFlags keyStorageFlags)
    {
        return new X509Certificate2(path, password, keyStorageFlags);
    }
}
