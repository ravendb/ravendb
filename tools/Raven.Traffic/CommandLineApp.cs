using System;
using System.IO;
using Microsoft.Extensions.CommandLineUtils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;

namespace Raven.Traffic
{
    internal static class CommandLineApp
    {
        private const string HelpOptionString = "-h | -? | --help";

        private static CommandLineApplication _app;

        public static int Run(string[] args)
        {
            if (args == null)
                throw new ArgumentNullException(nameof(args));

            _app = new CommandLineApplication
            {
                Name = "Raven.Traffic",
                Description = "This utility lets you record and replay traffic in RavenDB."
            };

            _app.HelpOption(HelpOptionString);

            ConfigureTrafficCommand();

            _app.OnExecute(() =>
            {
                _app.ShowHelp();
                return 1;
            });

            try
            {
                return _app.Execute(args);
            }
            catch (CommandParsingException parsingException)
            {
                return ExitWithError(parsingException.Message, _app);
            }
        }

        private static void ConfigureTrafficCommand()
        {
            _app.Command("rec", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Record database's traffic to a file";
                RunCommand(cmd, TrafficToolConfiguration.TrafficToolMode.Record);
            });
            
            _app.Command("play", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Replay traffic from a specified file to a database.";
                RunCommand(cmd, TrafficToolConfiguration.TrafficToolMode.Replay);
            });
        }

        private static void RunCommand(CommandLineApplication cmd, TrafficToolConfiguration.TrafficToolMode mode)
        {
            cmd.HelpOption(HelpOptionString);

            var urlArg = cmd.Argument("[url]", "RavenDB Server URL", cmdWithArg => { });
            var databaseArg = cmd.Argument("[database]", "Database name", cmdWithArg => { });
            var recordFilePathArg = cmd.Argument("[url]", "Record file path", cmdWithArg => { });

            var durationConstraintArg = cmd.Option("--trace-seconds", "Time to perform the traffic watch(seconds)", CommandOptionType.SingleValue);
            var amountConstraintArg = cmd.Option("--trace-requests", "Time to perform the traffic watch", CommandOptionType.SingleValue);
            var compressedArg = cmd.Option("--compressed", "Work with compressed json outpu/input", CommandOptionType.NoValue);
            var noOutputArg = cmd.Option("--noOutput", "Suppress console progress output", CommandOptionType.NoValue);
            var timeoutArg = cmd.Option("--timeout", "The timeout to use for requests(seconds)", CommandOptionType.SingleValue);

            cmd.OnExecute(() =>
            {
                var url = urlArg.Value;
                try
                {
                    // ReSharper disable once ObjectCreationAsStatement
                    new Uri(url);
                }
                catch (UriFormatException e)
                {
                    return ExitWithError($"Server's url provided isn't in valid format. {e}", cmd);
                }

                IDocumentStore store;
                try
                {
                    store = new DocumentStore
                    {
                        Urls = new[] {url},
                        Database = databaseArg.Value
                    }.Initialize();
                }
                catch (Exception e)
                {
                    return ExitWithError($"Could not connect to server. Exception: {e}", cmd);
                }

                using (store)
                {
                    try
                    {
                        store.Maintenance.Send(new GetStatisticsOperation());
                    }
                    catch (Exception)
                    {
                        return ExitWithError("Database does not exist.", cmd);
                    }
                    
                    var configuration = new TrafficToolConfiguration
                    {
                        Database = databaseArg.Value,
                        RecordFilePath = recordFilePathArg.Value,
                        Mode = mode,
                        DurationConstraint = TimeSpan.FromSeconds(durationConstraintArg.HasValue()
                            ? int.Parse(durationConstraintArg.Value())
                            : 60),
                        AmountConstraint = amountConstraintArg.HasValue()
                            ? int.Parse(amountConstraintArg.Value())
                            : 1000,
                    };
                    if (timeoutArg.HasValue())
                        configuration.Timeout = TimeSpan.FromSeconds(int.Parse(timeoutArg.Value()));
                    if (compressedArg.HasValue())
                        configuration.IsCompressed = true;
                    if (noOutputArg.HasValue())
                        configuration.PrintOutput = false;

                    new TrafficRec(store, configuration).ExecuteTrafficCommand();
                }

                return 0;
            });
        }

        private static int ExitWithError(string errMsg, CommandLineApplication cmd)
        {
            cmd.Error.WriteLine(errMsg);
            cmd.ShowHelp();
            return 1;
        }
    }
}
