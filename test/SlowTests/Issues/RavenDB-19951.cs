using System;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
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
    public async Task CanSetupTOTPWithoutLimits()
    {
        TestCertificatesHolder certificates = WithStore(out DocumentStore store);
        string key = TwoFactorAuthentication.GenerateSecret();
        await store.Maintenance.Server.SendAsync(
            new PutClientCertificateOperation("test",
                certificates.ClientCertificate1.Value,
                new(),
                SecurityClearance.Operator) {TwoFactorAuthenticationKey = key, TwoFactorAuthenticationValidityPeriod = TimeSpan.FromMinutes(5)});


        using (var firstClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
        {
            var e = await Assert.ThrowsAsync<Exception>(() => firstClient.ProtectedRequest());
            Assert.Contains("requires two factor authorization to be valid", e.Message);
            
            string validationCode = TwoFactorAuthentication.CreateValidationCode(key);
            await firstClient.PostToken(validationCode, false);
            
            await firstClient.ProtectedRequest();
            
            using (var secondClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
            {
                // not limits so auth is shared per client cert
                await secondClient.ProtectedRequest();
            }
        }
    }

    private TestCertificatesHolder WithStore(out DocumentStore store)
    {
        var certificates = Certificates.SetupServerAuthentication();
        store = GetDocumentStore(new Options {ClientCertificate = certificates.ServerCertificate.Value});
        Assert.NotNull(store.Certificate);
        return certificates;
    }

    [Fact]
    public async Task CanSetupTOTPWithLimits()
    {
        TestCertificatesHolder certificates = WithStore(out DocumentStore store);
        
        string key = TwoFactorAuthentication.GenerateSecret();
        await store.Maintenance.Server.SendAsync(
            new PutClientCertificateOperation("test",
                certificates.ClientCertificate1.Value,
                new(),
                SecurityClearance.Operator) {TwoFactorAuthenticationKey = key, TwoFactorAuthenticationValidityPeriod = TimeSpan.FromMinutes(5)});

        using (var firstClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
        {
            var e = await Assert.ThrowsAsync<Exception>(() => firstClient.ProtectedRequest());
            Assert.Contains("requires two factor authorization to be valid", e.Message);
            
            string validationCode = TwoFactorAuthentication.CreateValidationCode(key);
            await firstClient.PostToken(validationCode, true);
            
            await firstClient.ProtectedRequest();

            using (var secondClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
            {
                // with limits - so request should be reject due to lack of cookie
                var innerError = await Assert.ThrowsAsync<Exception>(() => secondClient.ProtectedRequest());
                Assert.Contains("requires two factor authorization to be valid", innerError.Message);
            }
        }
    }
    
    
    [Fact]
    public async Task CannotAccessServerAfterLogout()
    {
        TestCertificatesHolder certificates = WithStore(out DocumentStore store);
        
        string key = TwoFactorAuthentication.GenerateSecret();
        store.Maintenance.Server.Send(
            new PutClientCertificateOperation("test",
                certificates.ClientCertificate1.Value,
                new(),
                SecurityClearance.Operator) {TwoFactorAuthenticationKey = key, TwoFactorAuthenticationValidityPeriod = TimeSpan.FromMinutes(5)});

        using (var firstClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
        {
            var e = await Assert.ThrowsAsync<Exception>(() => firstClient.ProtectedRequest());
            Assert.Contains("requires two factor authorization to be valid", e.Message);

            string validationCode = TwoFactorAuthentication.CreateValidationCode(key);
            await firstClient.PostToken(validationCode, false);

            await firstClient.ProtectedRequest();

            using (var secondClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
            {
                // not limits so auth is shared per client cert
                await secondClient.ProtectedRequest();

                await firstClient.Logout();
                
                // both client won't be able to connect
                e = await Assert.ThrowsAsync<Exception>(() => firstClient.ProtectedRequest());
                Assert.Contains("requires two factor authorization to be valid", e.Message);
                
                e = await Assert.ThrowsAsync<Exception>(() => secondClient.ProtectedRequest());
                Assert.Contains("requires two factor authorization to be valid", e.Message);
            }
        }
    }
    
    [Fact]
    public async Task NewOtpOverridesOld()
    {
        TestCertificatesHolder certificates = WithStore(out DocumentStore store);
        
        string key = TwoFactorAuthentication.GenerateSecret();
        
        await store.Maintenance.Server.SendAsync(
            new PutClientCertificateOperation("test",
                certificates.ClientCertificate1.Value,
                new(),
                SecurityClearance.Operator) {TwoFactorAuthenticationKey = key, TwoFactorAuthenticationValidityPeriod = TimeSpan.FromMinutes(5)});

        using (var firstClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
        {
            string validationCode = TwoFactorAuthentication.CreateValidationCode(key);
            await firstClient.PostToken(validationCode, true);

            using (var secondClient = new TwoFactorClient(store, certificates.ClientCertificate1.Value))
            {
                validationCode = TwoFactorAuthentication.CreateValidationCode(key);
                await secondClient.PostToken(validationCode, true);
                
                // now second should be able to connect, but first can NOT
                
                await secondClient.ProtectedRequest();

                var e = await Assert.ThrowsAsync<Exception>(() => firstClient.ProtectedRequest());
                Assert.Contains("requires two factor authorization to be valid", e.Message);
            }
        }
    }

    class TwoFactorClient : IDisposable
    {
        private readonly HttpClientHandler _handler;

        private readonly HttpClient _client;

        private readonly DocumentStore _store;
        private readonly X509Certificate2 _certificate;

        public TwoFactorClient(DocumentStore store, X509Certificate2 certificate)
        {
            _handler = new HttpClientHandler { CookieContainer = new (), ServerCertificateCustomValidationCallback = (_, _, _, _) => true};
            _handler.ClientCertificates.Add(certificate);
            _client = new HttpClient(_handler);
            
            _store = store;
            _certificate = certificate;
        }

        public async Task PostToken(string code, bool withLimits)
        {
            var tokenRequest = new JsonObject();
            tokenRequest["Token"] = code;
            
            var content = new StringContent(tokenRequest.ToString(), Encoding.UTF8, "application/json");
            var url = _store.Urls[0] + "/authentication/2fa?hasLimits=" + withLimits;

            var response = await _client.PostAsync(url, content);
            await HandleResponse(response);
        }

        public async Task PublicRequest()
        {
            var response = await _client.GetAsync(_store.Urls[0] + "/studio/index.html");
            await HandleResponse(response);
        }

        public async Task ProtectedRequest()
        {
            var response = await _client.GetAsync(_store.Urls[0] + "/databases");
            await HandleResponse(response);
        }

        public async Task Logout()
        {
            var response = await _client.DeleteAsync(_store.Urls[0] + "/authentication/2fa");
            await HandleResponse(response);
        }

        private async Task HandleResponse(HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode == false)
            {
                var responseText = await response.Content.ReadAsStringAsync();
                throw new Exception(responseText);
            }
        }

        public void ClearCookies()
        {
            _handler.CookieContainer = new CookieContainer();
        }

        public void Dispose()
        {
            _client.Dispose();
            _handler.Dispose();
        }
    }
}
