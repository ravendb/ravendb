using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config;

namespace Raven.Server.Utils.Cli
{
    public static class PostSetupCliArgumentsUpdater
    {
        public static string[] Process(string[] configurationArgs, RavenConfiguration configBeforeRestart, RavenConfiguration currentConfiguration)
        {
            var result = configurationArgs;
            result = UpdateServerUrlCommandLineArgAfterSetupIfNecessary(result, configBeforeRestart.Core.ServerUrls, currentConfiguration.GetSetting(RavenConfiguration.GetKey(x => x.Core.ServerUrls)));
            result = FilterOutSetupModeArg(result);
            return result;
        }

        private static string[] UpdateServerUrlCommandLineArgAfterSetupIfNecessary(
            string[] originalCommandLineArgs, string[] oldServerUrl, string newServerUrl)
        {
            if (string.IsNullOrEmpty(newServerUrl))
                return originalCommandLineArgs;

            var idx = FindIndexOfCliOptFor(originalCommandLineArgs, RavenConfiguration.GetKey(x => x.Core.ServerUrls));
            if (idx == -1)
                return originalCommandLineArgs;

            var resultArgs = originalCommandLineArgs.ToArray();
            Uri.TryCreate(oldServerUrl[0], UriKind.Absolute, out var uriBeforeSetup);
            
            var newServerUrls = newServerUrl.Split(";");
            var uriBuilders = new List<UriBuilder>();
            foreach (var nServerUrl in newServerUrls)
            {
                Uri.TryCreate(nServerUrl, UriKind.Absolute, out var uriAfterSetup);

                if (string.Equals(uriAfterSetup.Scheme, uriBeforeSetup.Scheme, StringComparison.OrdinalIgnoreCase))
                {
                    uriBuilders.Add(new UriBuilder(uriAfterSetup));
                    continue;
                }

                uriBuilders.Add(new UriBuilder(uriAfterSetup.Scheme, uriBeforeSetup.Host, uriBeforeSetup.Port));
            }
            
            resultArgs[idx] = $"--{RavenConfiguration.GetKey(x => x.Core.ServerUrls)}={string.Join(";", uriBuilders.Select(builder => UrlUtil.TrimTrailingSlash(builder.ToString())))}";

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
                opt => possibleSetupModePrefixes.Any(opt.StartsWith));
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
