using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Utils.Cli
{
    public static class CommandLineConfigurationArgumentsHelper
    {
        public static string[] FormatCliArgPrefixes(string key)
        {
            return new []
            {
                $"--{key}=",
                $"/{key}="
            };
        }

        public static bool IsConfigurationKeyInCliArgs(string key, IEnumerable<string> cliArgs)
        {
            var optPrefixes = FormatCliArgPrefixes(key);
            return cliArgs.Any(arg => optPrefixes.Any(prefix => arg.StartsWith(prefix)));
        }

        public static int FindIndexOfCliOptFor(string[] args, string key)
        {
            var possibleSetupModePrefixes = FormatCliArgPrefixes(key);
            var idx = Array.FindIndex(args, 
                opt => possibleSetupModePrefixes.Any(prefix => opt.StartsWith(prefix)));
            return idx;
        }

    }
}
