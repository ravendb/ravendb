using Raven.Client;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_23795 : RavenTestBase
{
    public RavenDB_23795(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public void CertificateExtensionsOidBase_AndSnmpOidBase_AreDifferent()
    {
        var serverCertExtensionOidBase = Constants.Certificates.ServerCertExtensionOid.Substring(0, 19);
        var snmpOidBase = Constants.Monitoring.Snmp.SnmpRootOid.Substring(0, 19);
        Assert.NotEqual(serverCertExtensionOidBase, snmpOidBase);
    }
}
