using System;
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
        
        public bool RunningOnMacOsx { get; set; }

        public static SetupParameters Get(ServerStore serverStore)
        {
            var result = new SetupParameters();
            DetermineFixedPortNumber(serverStore, result);
            DetermineFixedTcpPortNumber(serverStore, result);

            result.IsDocker = PlatformDetails.RunningOnDocker;
            result.DockerHostname = result.IsDocker ? new Uri(serverStore.GetNodeHttpServerUrl()).Host : null;
            result.RunningOnMacOsx = PlatformDetails.RunningOnMacOsx;
            
            return result;
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
