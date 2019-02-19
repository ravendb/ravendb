using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils.Cli;
using Sparrow.Platform;

namespace Raven.Server.Commercial
{
    public class SetupParameters
    {
        public int? FixedServerPortNumber { get;set; }
        public int? FixedServerTcpPortNumber { get;set; }
        
        public bool IsDocker { get; set; }
        public string DockerHostname { get; set; }

        
        public bool IsAws { get; set; }
        public bool IsAzure { get; set; }
        
        public bool RunningOnPosix { get; set; }
        public bool RunningOnMacOsx { get; set; }

        private const string AzureUrl = "http://169.254.169.254/metadata/instance?api-version=2017-04-02";
        private const string AwsUrl = "http://instance-data.ec2.internal";
        private static readonly Lazy<HttpClient> HttpClient = new Lazy<HttpClient>(() => new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(5)
        });

        public static async Task<SetupParameters> Get(ServerStore serverStore)
        {
            var result = new SetupParameters();
            DetermineFixedPortNumber(serverStore, result);
            DetermineFixedTcpPortNumber(serverStore, result);

            result.IsDocker = PlatformDetails.RunningOnDocker;
            result.DockerHostname = result.IsDocker ? new Uri(serverStore.GetNodeHttpServerUrl()).Host : null;
            result.RunningOnMacOsx = PlatformDetails.RunningOnMacOsx;

            result.IsAws = await DetectIfRunningInAws();
            if (result.IsAws == false)
                result.IsAzure = await DetectIfRunningInAzure();

            result.RunningOnPosix = PlatformDetails.RunningOnPosix;
            
            return result;
        }

        private static async Task<bool> DetectIfRunningInAws()
        {
            try
            {
                var response = await HttpClient.Value.GetAsync(AwsUrl).ConfigureAwait(false);
                return response.IsSuccessStatusCode;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static async Task<bool> DetectIfRunningInAzure()
        {
            try
            {
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                    RequestUri = new Uri(AzureUrl),
                    Headers = {{ "Metadata", "true" }}
                };

                var response = await HttpClient.Value.SendAsync(request).ConfigureAwait(false);
                return response.IsSuccessStatusCode;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static void DetermineFixedPortNumber(ServerStore serverStore, SetupParameters result)
        {
            var serverUrlKey = RavenConfiguration.GetKey(x => x.Core.ServerUrls);
            var arguments = serverStore.Configuration.CommandLineSettings?.Args;
            if (arguments == null)
                return;

            if (CommandLineConfigurationArgumentsHelper.IsConfigurationKeyInCliArgs(serverUrlKey, arguments))
            {
                Uri.TryCreate(serverStore.Configuration.Core.ServerUrls[0], UriKind.Absolute, out var uri);
                result.FixedServerPortNumber = uri.Port;
            }
        }
        
        private static void DetermineFixedTcpPortNumber(ServerStore serverStore, SetupParameters result)
        {
            var serverUrlKey = RavenConfiguration.GetKey(x => x.Core.TcpServerUrls);
            var arguments = serverStore.Configuration.CommandLineSettings?.Args;
            if (arguments == null)
                return;

            if (CommandLineConfigurationArgumentsHelper.IsConfigurationKeyInCliArgs(serverUrlKey, arguments))
            {
                Uri.TryCreate(serverStore.Configuration.Core.TcpServerUrls[0], UriKind.Absolute, out var uri);
                result.FixedServerTcpPortNumber = uri.Port;
            }
        }
    }
}
