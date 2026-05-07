using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Utils;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Commercial.LetsEncrypt;

public sealed class LetsEncryptCertificateUtil
{
    internal static (byte[] CertBytes, CertificateDefinition CertificateDefinition, X509Certificate2 SelfSignedCertificate) GenerateClientCertificateTask(CertificateUtils.CertificateHolder certificateHolder, string certificateName, SetupInfo setupInfo)
    {
        if (certificateHolder.ServerCertificate == null)
            throw new InvalidOperationException($"Cannot generate the client certificate '{certificateName}' because the server certificate is not loaded.");

        // this creates a client certificate which is signed by the current server certificate
        var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificateName, certificateHolder.ServerCertificate, certificateHolder.PrivateKey, out var certBytes, setupInfo.ClientCertNotAfter ?? DateTime.UtcNow.Date.AddYears(5));

        var newCertDef = new CertificateDefinition
        {
            Name = certificateName,
            // this does not include the private key, that is only for the client
            Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
            Permissions = new Dictionary<string, DatabaseAccess>(),
            SecurityClearance = SecurityClearance.ClusterAdmin,
            Thumbprint = selfSignedCertificate.Thumbprint,
            PublicKeyPinningHash = selfSignedCertificate.GetPublicKeyPinningHash(),
            NotAfter = selfSignedCertificate.NotAfter,
            Usage = CertificateUsage.Client
        };

        return (certBytes, newCertDef, selfSignedCertificate);
    }

    public static async Task WriteCertificateAsPemToZipArchiveAsync(string name, byte[] rawBytes, string exportPassword, ZipArchive archive)
    {
        var cert = CertificateLoaderUtil.CreateCertificate(rawBytes, exportPassword, CertificateLoaderUtil.FlagsForExport);
        // Export the certificate to PEM
        var certPem = cert.ExportCertificatePem();
        var zipEntryCrt = archive.CreateEntry($"{name}.crt");
        zipEntryCrt.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;
        using (var entryStream = zipEntryCrt.Open())
        using (var writer = new StreamWriter(entryStream))
        {
            await writer.WriteAsync(certPem);
        }

        var zipEntryKey = archive.CreateEntry($"{name}.key");
        zipEntryKey.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;
        
        string keyPem;
        var privateKey = cert.GetExportableRsaPrivateKey();
        if (privateKey != null)
            keyPem = privateKey.ExportRSAPrivateKeyPem();
        else
            throw new CryptographicException("No RSA private key found");
        
        using (var entryStream = zipEntryKey.Open())
        using (var writer = new StreamWriter(entryStream))
        {
            await writer.WriteAsync(keyPem);
            await writer.FlushAsync();
        }
    }
}
