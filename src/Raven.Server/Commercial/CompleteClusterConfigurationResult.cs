using System.Security.Cryptography.X509Certificates;
using Raven.Client.ServerWide.Operations.Certificates;

namespace Raven.Server.Commercial;

public sealed class CompleteClusterConfigurationResult
{
    public string Domain;
    public byte[] CertBytes;
    public byte[] ServerCertBytes;
    public X509Certificate2 ServerCert;
    public X509Certificate2 ClientCert;
    public string PublicServerUrl;
    public CertificateDefinition CertificateDefinition;
}
