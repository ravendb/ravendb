using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Server.Config;

namespace Raven.Server.Utils.Cli
{
    public static class PostSetupCliArgumentsUpdater
    {
        public static string[] Process(string[] configurationArgs, RavenConfiguration configBeforeRestart, RavenConfiguration currentConfiguration)
        {
            var result = configurationArgs;
            result = UpdateServerUrlCommandLineArgAfterSetupIfNecessary(result, configBeforeRestart.Core.ServerUrl, currentConfiguration.GetSetting("ServerUrl"));
            result = FilterOutSetupModeArg(result);
            return result;
        }

        private static string[] UpdateServerUrlCommandLineArgAfterSetupIfNecessary(
            string[] originalCommandLineArgs, string oldServerUrl, string newServerUrl)
        {
            if (string.IsNullOrEmpty(newServerUrl))
                return originalCommandLineArgs;

            var idx = FindIndexOfCliOptFor(originalCommandLineArgs, RavenConfiguration.GetKey(x => x.Core.ServerUrl));
            if (idx == -1)
                return originalCommandLineArgs;

            var resultArgs = originalCommandLineArgs.ToArray();
            Uri.TryCreate(oldServerUrl, UriKind.Absolute, out var uriBeforeSetup);
            Uri.TryCreate(newServerUrl, UriKind.Absolute, out var uriAfterSetup);

            if (string.Equals(uriAfterSetup.Scheme, uriBeforeSetup.Scheme, StringComparison.InvariantCultureIgnoreCase))
                return originalCommandLineArgs;

            var uriBuilder = new UriBuilder(
                uriAfterSetup.Scheme, uriBeforeSetup.Host, uriBeforeSetup.Port);
            resultArgs[idx] = $"--{RavenConfiguration.GetKey(x => x.Core.ServerUrl)}={uriBuilder.ToString().TrimEnd('/')}";

            return resultArgs;
        }

        private static string[] FilterOutSetupModeArg(string[] args)
        {
            var idx = FindIndexOfCliOptFor(args, RavenConfiguration.GetKey(x => x.Core.SetupMode));
            if (idx == -1)
                return args;

            var result = args.ToList();
            result.RemoveAt(idx);
            return result.ToArray();
        }

        private static int FindIndexOfCliOptFor(string[] args, string key)
        {
            var possibleSetupModePrefixes = FormatCliArgPrefixes(key);
            var idx = Array.FindIndex(args, 
                opt => possibleSetupModePrefixes.Any(prefix => opt.StartsWith(prefix)));
            return idx;
        }

        private static string[] FormatCliArgPrefixes(string key)
        {
            return new []
            {
                $"--{key}=",
                $"/{key}="
            };
        }

    }
}
