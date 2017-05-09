using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Extensions;

namespace Raven.Server.Utils
{
    internal class CommandLineSwitches
    {
        public static bool PrintServerId { get; private set; }

        public static bool LaunchBrowser { get; private set; }

        public static bool PrintVersionAndExit { get; private set; }

        public static string[] ParseAndRemove(string[] args)
        {
            if (args == null)
                return null;

            var list = args.ToList();
            PrintServerId = list.Remove("--print-id");
            LaunchBrowser = list.Remove("--browser");
            PrintVersionAndExit = list.Remove("-v") || list.Remove("--version");
            return list.ToArray();
        }
    }
}
