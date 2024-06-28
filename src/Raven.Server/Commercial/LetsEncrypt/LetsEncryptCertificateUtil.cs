using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography.Pkcs;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Pkcs;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Utils;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Commercial.LetsEncrypt;

public sealed class LetsEncryptCertificateUtil
{
    internal static (byte[] CertBytes, CertificateDefinition CertificateDefinition, X509Certificate2 SelfSignedCertificate) GenerateClientCertificateTask(CertificateUtils.CertificateHolder certificateHolder, string certificateName, SetupInfo setupInfo)
    {
        if (certificateHolder.Certificate == null)
            throw new InvalidOperationException($"Cannot generate the client certificate '{certificateName}' because the server certificate is not loaded.");

        // this creates a client certificate which is signed by the current server certificate
        var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificateName, certificateHolder, out var certBytes, setupInfo.ClientCertNotAfter ?? DateTime.UtcNow.Date.AddYears(5));

        var newCertDef = new CertificateDefinition
        {
            Name = certificateName,
            // this does not include the private key, that is only for the client
            Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
            Permissions = new Dictionary<string, DatabaseAccess>(),
            SecurityClearance = SecurityClearance.ClusterAdmin,
            Thumbprint = selfSignedCertificate.Thumbprint,
            PublicKeyPinningHash = selfSignedCertificate.GetPublicKeyPinningHash(),
            NotAfter = selfSignedCertificate.NotAfter
        };

        return (certBytes, newCertDef, selfSignedCertificate);
    }

    public static async Task WriteCertificateAsPemToZipArchiveAsync(string name, byte[] rawBytes, string exportPassword, ZipArchive archive)
    {
        var a = new Pkcs12StoreBuilder().Build();
        a.Load(new MemoryStream(rawBytes), Array.Empty<char>());

        X509CertificateEntry entry = null;
        AsymmetricKeyEntry key = null;
        foreach (var alias in a.Aliases)
        {
            var aliasKey = a.GetKey(alias.ToString());
            if (aliasKey != null)
            {
                entry = a.GetCertificate(alias.ToString());
                key = aliasKey;
                break;
            }
        }

        if (entry == null)
        {
            throw new InvalidOperationException("Could not find private key.");
        }

        var zipEntryCrt = archive.CreateEntry(name + ".crt");
        zipEntryCrt.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

        await using (var stream = zipEntryCrt.Open())
        await using (var writer = new StreamWriter(stream))
        {
            var pw = new PemWriter(writer);
            pw.WriteObject(entry.Certificate);
        }

        var zipEntryKey = archive.CreateEntry(name + ".key");
        zipEntryKey.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

        await using (var stream = zipEntryKey.Open())
        await using (var writer = new StreamWriter(stream))
        {
            var pw = new PemWriter(writer);

            object privateKey;
            if (exportPassword != null)
            {
                privateKey = new MiscPemGenerator(
                        key.Key,
                        "DES-EDE3-CBC",
                        exportPassword.ToCharArray(),
                        CertificateUtils.GetSeededSecureRandom())
                    .Generate();
            }
            else
            {
                privateKey = key.Key;
            }

            pw.WriteObject(privateKey);

            await writer.FlushAsync();
        }
    }
}
