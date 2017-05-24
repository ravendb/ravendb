using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.CommandLineUtils;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    internal class CommandLineSwitches
    {
        public static bool PrintServerId => ParseSwitchOption(_printIdOption);

        public static bool LaunchBrowser => ParseSwitchOption(_browserOption);

        public static bool PrintVersionAndExit => ParseSwitchOption(_versionOption);

        public static bool RunAsService => ParseSwitchOption(_runAsServiceOption);

        public static bool RegisterService => ParseSwitchOption(_registerServiceOption);

        public static bool UnregisterService => ParseSwitchOption(_unregisterServiceOption);

        public const string DefaultServiceName = "RavenDB";

        public static string ServiceName => 
            _serviceNameOption.Values.FirstOrDefault() ?? DefaultServiceName;

        public static string CustomConfigPath =>
            _customConfigPathOption.Values.FirstOrDefault();

        public static bool ShouldShowHelp => ParseSwitchOption(_helpOption);

        private static CommandLineApplication _app;

        private static CommandOption _printIdOption;

        private static CommandOption _browserOption;

        private static CommandOption _versionOption;

        private static CommandOption _registerServiceOption;

        private static CommandOption _unregisterServiceOption;

        private static CommandOption _runAsServiceOption;

        private static CommandOption _serviceNameOption;

        private static CommandOption _helpOption;

        private static CommandOption _customConfigPathOption;

        public static void ShowHelp()
        {
            _app.ShowHelp();
        }

        public static string[] Process(string[] args)
        {
            if (args == null)
                return null;

            var nonConfigurationSwitches = args
                .Where(x => Regex.IsMatch(x, @"^(--|\/)[rR]aven") == false)
                .ToArray();

            _app = new CommandLineApplication();
            _helpOption = _app.Option(
                "-h | -? | --help",
                "Shows help",
                CommandOptionType.NoValue);
            _versionOption = _app.Option(
                "-v | --version",
                "Displays version and exits",
                CommandOptionType.NoValue);
            _printIdOption = _app.Option(
                "--print-id", 
                "Prints server ID upon server start", 
                CommandOptionType.NoValue);
            _registerServiceOption = _app.Option(
                "--register-service",
                "Registers Windows service and exits",
                CommandOptionType.NoValue);
            _unregisterServiceOption = _app.Option(
                "--unregister-service",
                "Unregisters Windows service and exits",
                CommandOptionType.NoValue);
            _runAsServiceOption = _app.Option(
                "--run-as-service",
                "Runs as service (available only for Linux). Windows users should use --register-service and services.msc for service management",
                CommandOptionType.NoValue);
            _serviceNameOption = _app.Option(
                "--service-name",
                "Sets service name",
                CommandOptionType.SingleValue);
            _customConfigPathOption = _app.Option(
                "-c | --config-path",
                "Sets custom configuration file path",
                CommandOptionType.SingleValue);
            _browserOption = _app.Option(
                "--browser", 
                "Attempts to open RavenDB Studio in the browser", 
                CommandOptionType.NoValue);

            _app.Execute(nonConfigurationSwitches);

            Validate();

            return args.Except(nonConfigurationSwitches).ToArray();
        }

        private static void Validate()
        {
            if (ServiceName.Length > 256)
                throw new CommandParsingException(_app, "Service name must have maximum length of 256 characters.");

            if (PlatformDetails.RunningOnPosix == false && RunAsService)
                throw new CommandParsingException(_app, "Switch \"--run-as-service\" is not supported on Windows. Use --register-service switch to register the service and services.msc for service management.");
        }

        private static bool ParseSwitchOption(CommandOption opt)
        {
            var val = opt.Values.FirstOrDefault();
            if (val == "on")
            {
                return true;
            }

            bool result = false;
            bool.TryParse(val, out result);
            return result;
        }
    }
}
