using System;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_8097 : NoDisposalNeeded
    {
        [Fact]
        public void TcpServerUrlShouldOnlyAllowTcpScheme()
        {
            GetConfiguration(tcpServerUrl: "tcp://test.info:123");
            try
            {
                GetConfiguration(tcpServerUrl: "tpp://test.info:123");
            }
            catch (ArgumentException argException) when (argException.Message.StartsWith("URI scheme"))
            {
            }
        }

        [Fact]
        public void PortNumberIsAValidTcpServerUrl()
        {
            GetConfiguration(tcpServerUrl: "38888");
        }

        [Fact]
        public void ServerUrlShouldOnlyAllowHttpOrHttps()
        {
            GetConfiguration("http://test.com");
            GetConfiguration("https://192.152.23.3:345", certPath: "certPath.pem");

            foreach (var serverUrl in new string[]
            {
                "hhtp://ravendb.net:1234",
                "zxcv://ravendb.net:1234"
            })
            {
                try
                {
                    GetConfiguration(serverUrl);
                }
                catch (ArgumentException argException) when (argException.Message.StartsWith("URI scheme"))
                {
                }
            }
        }

        [Fact]
        public void ServerUrlShouldBeValidUri()
        {
            try
            {
                GetConfiguration("this:/isnota_valid.uri");
            }
            catch (ArgumentException argException) when (argException.Message.StartsWith("URI scheme"))
            {
            }
        }

        public RavenConfiguration GetConfiguration(string serverUrl = null, string tcpServerUrl = null, string certPath = null)
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.ServerUrls), serverUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.TcpServerUrls), tcpServerUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Security.CertificatePath), certPath);

            configuration.Initialize();

            return configuration;
        }
    }
}
