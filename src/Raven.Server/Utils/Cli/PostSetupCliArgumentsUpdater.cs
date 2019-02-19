using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config;
using Sparrow.Platform;

namespace Raven.Server.Utils.Cli
{
    public static class PostSetupCliArgumentsUpdater
    {
        public static string[] Process(string[] configurationArgs, RavenConfiguration configBeforeRestart, RavenConfiguration currentConfiguration)
        {
            var result = configurationArgs;
            result = UpdateServerUrlCommandLineArgAfterSetupIfNecessary(result, configBeforeRestart.Core.ServerUrls, currentConfiguration.GetSetting(RavenConfiguration.GetKey(x => x.Core.ServerUrls)));
            result = FilterOutSetupModeArg(result);
            result = FilterOutUnsecuredAccessAllowedIfNeeded(result);
            return result;
        }

        private static string[] UpdateServerUrlCommandLineArgAfterSetupIfNecessary(
            string[] originalCommandLineArgs, string[] oldServerUrl, string newServerUrl)
        {
            if (string.IsNullOrEmpty(newServerUrl))
                return originalCommandLineArgs;

            var idx = CommandLineConfigurationArgumentsHelper.FindIndexOfCliOptFor(
                originalCommandLineArgs, RavenConfiguration.GetKey(x => x.Core.ServerUrls));
            if (idx == -1)
                return originalCommandLineArgs;

            var resultArgs = originalCommandLineArgs.ToArray();
            Uri.TryCreate(oldServerUrl[0], UriKind.Absolute, out var uriBeforeSetup);
            
            var newServerUrls = newServerUrl.Split(";");
            var uriBuilders = new List<UriBuilder>();
            foreach (var nServerUrl in newServerUrls)
            {
                Uri.TryCreate(nServerUrl, UriKind.Absolute, out var uriAfterSetup);

                uriBuilders.Add(new UriBuilder(uriAfterSetup.Scheme, uriBeforeSetup.Host, uriBeforeSetup.Port));
            }
            
            resultArgs[idx] = $"--{RavenConfiguration.GetKey(x => x.Core.ServerUrls)}={string.Join(";", uriBuilders.Select(builder => UrlUtil.TrimTrailingSlash(builder.ToString())))}";

            return resultArgs;
        }

        private static string[] FilterOutSetupModeArg(string[] args)
        {
            return FilterOutArgByConfigurationKey(args, RavenConfiguration.GetKey(x => x.Core.SetupMode));
        }

        private static string[] FilterOutArgByConfigurationKey(string[] args, string confKey)
        {
            var idx = CommandLineConfigurationArgumentsHelper.FindIndexOfCliOptFor(args, confKey);
            if (idx == -1)
                return args;

            var result = args.ToList();
            result.RemoveAt(idx);
            return result.ToArray();
        }

        private static string[] FilterOutUnsecuredAccessAllowedIfNeeded(string[] args)
        {
            var removeArgEnvVar = Environment.GetEnvironmentVariable("REMOVE_UNSECURED_CLI_ARG_AFTER_RESTART");
            var shouldRemoveUnsecuredCliArg = removeArgEnvVar == "true";

            if (PlatformDetails.RunningOnDocker && shouldRemoveUnsecuredCliArg)
            {
                return FilterOutArgByConfigurationKey(args, RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed));
            }

            return args;
        }

    }
}
