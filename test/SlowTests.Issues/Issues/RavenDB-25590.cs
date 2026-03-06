using System;
using FastTests;
using Raven.Client.Util;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25590 : RavenTestBase
{
    public RavenDB_25590(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void SerialNumberIsNotLongerThan20Bytes()
    {
        for (int i = 0; i < 10; i++)
        {
            var guid = Guid.NewGuid().ToString().Split('-')[0];
            var certBytes = CertificateUtils.CreateSelfSignedTestCertificate($"RavenDB_25590-{guid}", $"RavenDB_25590_CA-{guid}");
            
            using (var cert = CertificateLoaderUtil.CreateCertificate(certBytes))
            {
                var serialNumber = cert.GetSerialNumber();
                
                Assert.True(serialNumber.Length <= 20, 
                    $"Serial number length is {serialNumber.Length} bytes, which exceeds the 20-byte limit. Serial: {cert.SerialNumber}");
            }
        }
    }
}
