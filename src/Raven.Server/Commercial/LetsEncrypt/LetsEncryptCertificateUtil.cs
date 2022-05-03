using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Pkcs;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Utils;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Commercial.LetsEncrypt;

public class LetsEncryptCertificateUtil
{
    public static string GetServerUrlFromCertificate(X509Certificate2 cert, SetupInfo setupInfo, string nodeTag, int port, int tcpPort, out string publicTcpUrl, out string domain)
        {
            publicTcpUrl = null;
            var node = setupInfo.NodeSetupInfos[nodeTag];

            var cn = cert.GetNameInfo(X509NameType.SimpleName, false);
            Debug.Assert(cn != null, nameof(cn) + " != null");
            if (cn[0] == '*')
            {
                var parts = cn.Split("*.");
                if (parts.Length != 2)
                    throw new FormatException($"{cn} is not a valid wildcard name for a certificate.");

                domain = parts[1];

                publicTcpUrl = node.ExternalTcpPort != 0
                    ? $"tcp://{nodeTag.ToLower()}.{domain}:{node.ExternalTcpPort}"
                    : $"tcp://{nodeTag.ToLower()}.{domain}:{tcpPort}";

                if (setupInfo.NodeSetupInfos[nodeTag].ExternalPort != 0)
                    return $"https://{nodeTag.ToLower()}.{domain}:{node.ExternalPort}";

                return port == 443
                    ? $"https://{nodeTag.ToLower()}.{domain}"
                    : $"https://{nodeTag.ToLower()}.{domain}:{port}";
            }

            domain = cn; //default for one node case

            foreach (var value in GetCertificateAlternativeNames(cert))
            {
                if (value.StartsWith(nodeTag + ".", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                domain = value;
                break;
            }

            var url = $"https://{domain}";

            if (node.ExternalPort != 0)
                url += ":" + node.ExternalPort;
            else if (port != 443)
                url += ":" + port;

            publicTcpUrl = node.ExternalTcpPort != 0
                ? $"tcp://{domain}:{node.ExternalTcpPort}"
                : $"tcp://{domain}:{tcpPort}";

            node.PublicServerUrl = url;
            node.PublicTcpServerUrl = publicTcpUrl;

            return url;
        }

    public static IEnumerable<string> GetCertificateAlternativeNames(X509Certificate2 cert)
    {
        // If we have alternative names, find the appropriate url using the node tag
        var sanNames = cert.Extensions["2.5.29.17"];

        if (sanNames == null)
            yield break;

        var generalNames = GeneralNames.GetInstance(Asn1Object.FromByteArray(sanNames.RawData));

        foreach (var certHost in generalNames.GetNames())
        {
            yield return certHost.Name.ToString();
        }
    }

    internal static (byte[] CertBytes, CertificateDefinition CertificateDefinition) GenerateCertificate(CertificateUtils.CertificateHolder certificateHolder, string certificateName, SetupInfo setupInfo)
    {
        if (certificateHolder == null)
            throw new InvalidOperationException(
                $"Cannot generate the client certificate '{certificateName}' because the server certificate is not loaded.");

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

        return (certBytes, newCertDef);
    }
    
    public static async Task WriteCertificateAsPemAsync(string name, byte[] rawBytes, string exportPassword, ZipArchive archive)
        {
            var a = new Pkcs12Store();
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
