using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.Operations;
using Microsoft.Extensions.CommandLineUtils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Sparrow.Logging;

namespace Voron.Recovery
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
                Name = "Voron.Recovery",
                Description = "Recovery utility for Voron."
            };

            _app.HelpOption(HelpOptionString);

            ConfigureRecoveryCommand();

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

        
        private const string DatafileName = "Raven.voron";
        private const string RecoveryFileName = "recovery.ravendump";
        
        private static void ConfigureRecoveryCommand()
        {
            _app.Command("recover", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Recovering a database into recovery.ravendump.";
                cmd.HelpOption(HelpOptionString);

                var dataFileDirectoryArg = cmd.Argument("DataFileDirectory", "The database directory which contains the data file");
                var recoverDirectoryArg = cmd.Argument("RecoverDirectory", "The directory to recover the recovery.ravendump file");

                var outputFileNameArg = cmd.Option("--OutputFileName", "Will overwrite the default file name () with your own file name (under output directory).", CommandOptionType.SingleValue);
                var pageSizeInKbArg = cmd.Option("--PageSizeInKB", "Will set the recovery tool to work with page sizes other than 4kb.", CommandOptionType.SingleValue);
                var initialContextSizeInMbArg = cmd.Option("--InitialContextSizeInMB", "Will set the recovery tool to use a context of the provided size in MB.", CommandOptionType.SingleValue);
                var initialContextLongLivedSizeInKbArg = cmd.Option("--InitialContextLongLivedSizeInKB", "Will set the recovery tool to use a long lived context size of the provided size in KB.", CommandOptionType.SingleValue);
                var progressIntervalInSecArg = cmd.Option("--ProgressIntervalInSec", "Will set the recovery tool refresh to console rate interval in seconds.", CommandOptionType.SingleValue);
                var disableCopyOnWriteModeArg = cmd.Option("--DisableCopyOnWriteMode", "Default is false.", CommandOptionType.SingleValue);
                var loggingModeArg = cmd.Option("--LoggingMode", "Logging mode: Operations or Information.", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    VoronRecoveryConfiguration config = new VoronRecoveryConfiguration
                    {
                        DataFileDirectory = dataFileDirectoryArg.Value,
                    };

                    if (string.IsNullOrWhiteSpace(config.DataFileDirectory) ||
                        Directory.Exists(config.DataFileDirectory) == false ||
                        File.Exists(Path.Combine(config.DataFileDirectory, DatafileName)) == false)
                    {
                        return ExitWithError($"Missing {nameof(config.DataFileDirectory)} argument", cmd);
                    }
                    config.PathToDataFile = Path.Combine(config.DataFileDirectory, DatafileName);

                    var recoverDirectory = recoverDirectoryArg.Value;
                    if (string.IsNullOrWhiteSpace(recoverDirectory))
                    {
                        return ExitWithError("Missing RecoverDirectory argument", cmd);
                    }
                    
                    config.OutputFileName = Path.Combine(recoverDirectory, outputFileNameArg.HasValue() ? outputFileNameArg.Value() : RecoveryFileName);
                    try
                    {
                        if (!Directory.Exists(recoverDirectory))
                            Directory.CreateDirectory(recoverDirectory);
                        File.WriteAllText(config.OutputFileName, "I have write permission!");
                        File.Delete(config.OutputFileName);
                    }
                    catch
                    {
                        return ExitWithError($"Cannot write to the output directory ({recoverDirectory}). " +
                                             "Permissions issue?", cmd);
                    }

                    if (pageSizeInKbArg.HasValue())
                    {
                        if (int.TryParse(pageSizeInKbArg.Value(), out var pageSize) == false ||
                            pageSize < 1)
                            return ExitWithError($"{nameof(config.PageSizeInKB)} argument value ({pageSize}) is invalid", cmd);
                        config.PageSizeInKB = pageSize;
                    }

                    if (initialContextSizeInMbArg.HasValue())
                    {
                        if (int.TryParse(initialContextSizeInMbArg.Value(), out var contextSize) == false ||
                            contextSize < 1)
                            return ExitWithError($"{nameof(config.InitialContextSizeInMB)} argument value ({contextSize}) is invalid", cmd);
                        config.InitialContextSizeInMB = contextSize;
                    }

                    if (initialContextLongLivedSizeInKbArg.HasValue())
                    {
                        if (int.TryParse(initialContextLongLivedSizeInKbArg.Value(), out var longLivedContextSize) == false ||
                            longLivedContextSize < 1)
                            return ExitWithError($"{nameof(config.InitialContextLongLivedSizeInKB)} argument value ({longLivedContextSize}) is invalid", cmd);
                        config.InitialContextLongLivedSizeInKB = longLivedContextSize;
                    }

                    if (progressIntervalInSecArg.HasValue())
                    {
                        if (int.TryParse(progressIntervalInSecArg.Value(), out var refreshRate) == false ||
                            refreshRate < 1)
                            return ExitWithError($"{nameof(config.ProgressIntervalInSec)} argument value ({refreshRate}) is invalid", cmd);
                        config.ProgressIntervalInSec = refreshRate;
                    }

                    if (disableCopyOnWriteModeArg.HasValue())
                    {
                        var value = disableCopyOnWriteModeArg.Value();
                        if (bool.TryParse(value, out var disableCopyOnWriteMode) == false)
                            return ExitWithError($"{nameof(config.DisableCopyOnWriteMode)} argument value ({value}) is invalid", cmd);
                        config.DisableCopyOnWriteMode = disableCopyOnWriteMode;
                    }

                    if (loggingModeArg.HasValue())
                    {
                        var value = loggingModeArg.Value();
                        if (Enum.TryParse(value, out LogMode mode) == false)
                            return ExitWithError($"{nameof(config.LoggingMode)} argument value ({value}) is invalid", cmd);
                        config.LoggingMode = mode;
                    }

                    using (var recovery = new Recovery(config))
                    {
                        var cts = new CancellationTokenSource();
                        Console.WriteLine("Press 'q' to quit the recovery process");
                        var cancellationTask = Task.Factory.StartNew(() =>
                        {
                            while (Console.Read() != 'q')
                            {
                            }

                            cts.Cancel();
                            //The reason i do an exit here is because if we are in the middle of journal recovery 
                            //we can't cancel it and it may take a long time.
                            //That said i'm still going to give it a while to do a proper exit
                            Task.Delay(5000).ContinueWith(_ => { Environment.Exit(1); });
                        }, cts.Token);
                        recovery.Execute(Console.Out, cts.Token);
                        cts.Cancel();
                    }

                    return 0;
                });
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
