using System.Security.Cryptography.X509Certificates;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Utils;

namespace Raven.Server.Commercial;

public class CompleteClusterConfigurationResult
{
    public string Domain;
    public byte[] CertBytes;
    public byte[] ServerCertBytes;
    public X509Certificate2 ServerCert;
    public X509Certificate2 ClientCert;
    public string PublicServerUrl;
    public CertificateDefinition CertificateDefinition;
}
