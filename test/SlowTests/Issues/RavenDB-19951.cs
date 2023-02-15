using System;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Web.Authentication;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19951 : RavenTestBase
{
    public RavenDB_19951(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanSetupTOTP()
    {
        var certificates = Certificates.SetupServerAuthentication();
        using var store = GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificate.Value,
        });
        Assert.NotNull(store.Certificate);
        string key = TwoFactorAuthentication.GenerateSecret();
        store.Maintenance.Server.Send(
            new PutClientCertificateOperation("test", 
                certificates.ClientCertificate1.Value, 
                new(),
                SecurityClearance.Operator)
        {
            TwoFactorAuthenticationKey = key,
            TwoFactorAuthenticationValidityPeriod = TimeSpan.FromMinutes(5)
        });
        {
              
            using var withoutTotp = new DocumentStore
            {
                Certificate = certificates.ClientCertificate1.Value,
                Database = store.Database,
                Urls = store.Urls,
            }.Initialize();
            
            
            using (var s = withoutTotp.OpenSession())
            {
                var e = Assert.Throws<AuthorizationException>(() => s.Load<object>("item/1"));
                Assert.Contains("requires two factor authorization to be valid", e.Message);
            }

        }
        using var storeTotp = new DocumentStore
        {
            Certificate = certificates.ClientCertificate1.Value,
            Database = store.Database,
            Urls = store.Urls,
            Conventions =
            {
                // have to do that to avoid /cluster/topology failure
                DisableTopologyUpdates = true 
            }
        }.Initialize();

        string validationCode = TwoFactorAuthentication.CreateValidationCode(key);

        storeTotp.Maintenance.Server.Send(new ValidateTwoFactorAuthenticationTokenOperation(validationCode));

        using (var s = storeTotp.OpenSession())
        {
            s.Load<object>("item/1");
        }
        
        using var anotherStore = new DocumentStore // now we try another connection
        {
            Certificate = certificates.ClientCertificate1.Value,
            Database = store.Database,
            Urls = store.Urls,
        }.Initialize();
        using (var s = anotherStore.OpenSession())
        {
            s.Load<object>("item/1");
        }

    }
}
