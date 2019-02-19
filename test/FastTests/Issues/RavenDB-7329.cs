using System;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_7329 : NoDisposalNeeded
    {
        private const string FakeCertPath = "C:\\fake\\cert\\path.crt";

        [Fact]
        public void GivenZerosInServerUrlShouldUseWebUriForNodeUrl()
        {
            var config = GetConfiguration(serverUrl: "http://0.0.0.0:8080",
                unsecuredAccessAddressRange: nameof(UnsecuredAccessAddressRange.PublicNetwork));
            var result = config.Core.GetNodeHttpServerUrl("http://localhost:8080");
            Assert.Equal($"http://localhost:8080".ToLowerInvariant(), result);
        }

        [Fact]
        public void GivenNonZeroAddressReturnsServersWebUrl()
        {
            var config = GetConfiguration(serverUrl: "http://localhost:0");
            var result = config.Core.GetNodeHttpServerUrl("http://localhost:8888");
            Assert.Equal($"http://localhost:8888".ToLowerInvariant(), result);
        }

        [Fact]
        public void GivenSetPublicServerShouldUseThatForNodeUrl()
        {
            var config = GetConfiguration(
                serverUrl: "http://0.0.0.0:8080",
                publicServerUrl: "http://live-test.ravendb.net:80",
                unsecuredAccessAddressRange: nameof(UnsecuredAccessAddressRange.PublicNetwork));
            var result = config.Core.GetNodeHttpServerUrl("http://localhost:8080");
            Assert.Equal($"http://live-test.ravendb.net:80".ToLowerInvariant(), result);
        }

        [Fact]
        public void GivenPortZeroInTcpServerUrlShouldTakeItFromArg()
        {
            var config = GetConfiguration(
                serverUrl: "http://0.0.0.0:8080", 
                unsecuredAccessAddressRange: nameof(UnsecuredAccessAddressRange.PublicNetwork));
            var result = config.Core.GetNodeTcpServerUrl("http://localhost:8080", 38888);
            Assert.Equal($"tcp://localhost:38888".ToLowerInvariant(), result);
        }

        [Fact]
        public void GivenPublicTcpServerUrlItShouldUseThatForNodeTcpServerUrl()
        {
            var config = GetConfiguration(publicTcpServerUrl: "tcp://live-test.ravendb.net:55555");
            var result = config.Core.GetNodeTcpServerUrl("http://localhost:8080", 37777);
            Assert.Equal($"tcp://live-test.ravendb.net:55555".ToLowerInvariant(), result);
        }

        [Fact]
        public void PublicUrlShouldNotBeZeros()
        {
            try
            {
                GetConfiguration(serverUrl: "http://0.0.0.0:8080", publicServerUrl: "http://0.0.0.0:40000");
                throw new Exception("Configuration should have been validated.");
            }
            catch (ArgumentException argException)
            {
                Assert.Equal($"Invalid host value in {RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)} configuration option: 0.0.0.0", argException.Message);
            }
        }

        [Fact]
        public void PublicUrlShouldHaveSameSchemeAsBindingOne()
        {
            try
            {
                GetConfiguration(
                    publicServerUrl: "http://localhost:8080", 
                    serverUrl: "https://localhost:8080", 
                    certPath: FakeCertPath);
                throw new Exception("Configuration should have been validated.");
            }
            catch (ArgumentException argException)
            {
                Assert.Contains( "ServerUrl and PublicServerUrl schemes do not match:", argException.Message);
            }
        }

        [Fact]
        public void PublicUrlShouldNotHavePortZero()
        {
            try
            {
                GetConfiguration(serverUrl: "http://0.0.0.0:8080", publicServerUrl: "http://0.0.0.0:0");
                throw new Exception("Configuration should have been validated.");
            }
            catch (ArgumentException argException)
            {
                Assert.Equal($"Invalid port value in {RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)} configuration option: 0.", argException.Message);
            }
        }

        [Fact]
        public void PublicTcpUrlShouldNotBeZeros()
        {
            try
            {
                GetConfiguration(
                    tcpServerUrl: "tcp://0.0.0.0:8080", publicTcpServerUrl: "http://0.0.0.0:40000");
                throw new Exception("Configuration should have been validated.");
            }
            catch (ArgumentException argException)
            {
                Assert.Equal($"Invalid host value in {RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)} configuration option: 0.0.0.0", argException.Message);
            }
        }

        [Fact]
        public void PublicTcpUrlShouldNotHavePortZero()
        {
            try
            {
                GetConfiguration(tcpServerUrl: "tcp://0.0.0.0:8080", publicTcpServerUrl: "http://0.0.0.0:0");
                throw new Exception("Configuration should have been validated.");
            }
            catch (ArgumentException argException)
            {
                Assert.Equal($"Invalid port value in {RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)} configuration option: 0.", argException.Message);
            }
        }

        [Fact]
        public void UrlSchemeShouldPassThrough()
        {
            var config = GetConfiguration(serverUrl: "https://localhost:8080", certPath: FakeCertPath);
            var result = config.Core.GetNodeHttpServerUrl("https://localhost:8080");
            Assert.Equal($"https://localhost:8080".ToLowerInvariant(), result);
        }

        public RavenConfiguration GetConfiguration(string publicServerUrl = null, string publicTcpServerUrl = null, string serverUrl = null, string tcpServerUrl = null, string certPath = null, string unsecuredAccessAddressRange = nameof(UnsecuredAccessAddressRange.Local))
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.ServerUrls), serverUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), publicServerUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl), publicTcpServerUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.TcpServerUrls), tcpServerUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Security.CertificatePath), certPath);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed), unsecuredAccessAddressRange);
            configuration.Initialize();

            return configuration;
        }
    }
}
