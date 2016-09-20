using System;
using System.Threading;
using System.Threading.Tasks;

namespace Voron.Recovery
{
    public class Program
    {
        public static void Main(string[] args)
        {
            VoronRecoveryConfiguration config;
            switch (VoronRecoveryConfiguration.ProcessArgs(args, out config))
            {
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.Success:
                    break;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.NotEnoughArguments:
                    PrintUsage();
                    return;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.MissingDataFile:
                    PrintUsage();
                    return;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.CantWriteToOutputDirectory:
                    PrintUsage();
                    return;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.WrongNumberOfArgs:
                    Console.WriteLine($"The given amount of args doesn't dived by 2 like expected.{Environment.NewLine}");
                    goto default;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.InvalidPageSize:
                    Console.WriteLine($"Page size should be a positive number.{Environment.NewLine}");
                    goto default;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.InvalidTableValueCount:
                    Console.WriteLine($"Table value count should be a positive number.{Environment.NewLine}");
                    goto default;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.InvalidContextSize:
                    Console.WriteLine($"Context size should be a positive number.{Environment.NewLine}");
                    goto default;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.InvalidLongLivedContextSize:
                    Console.WriteLine($"Long lived size should be a positive number.{Environment.NewLine}");
                    goto default;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.InvalidRefreshRate:
                    Console.WriteLine($"Refresh rate should be a positive number.{Environment.NewLine}");
                    goto default;
                case VoronRecoveryConfiguration.VoronRecoveryArgsProcessStatus.BadArg:
                    Console.WriteLine($"Unexpected argument provided.{Environment.NewLine}");
                    goto default;
                default:
                    PrintUsage();
                    return;
            }
            var recovery = new Recovery(config);
            var cts = new CancellationTokenSource();
            Console.WriteLine("Press 'q' to quit the recovery process");
            var cancellationTask = Task.Factory.StartNew(() =>
            {                
                while (Console.Read() != 'q')
                {
                }
                cts.Cancel();
            }, cts.Token);
            recovery.Execute(cts.Token);
            cts.Cancel();
        }

        private static void PrintUsage()
        {
            Console.ForegroundColor = ConsoleColor.DarkMagenta;
            Console.WriteLine(@"
Recovery utility for voron
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------", DateTime.UtcNow.Year);
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine(@"
Usage:
    Voron.Recovery <Database folder> <Output directory> [[Option 0] [Option 1] ...]

Examples:
    - Recovering Northwind database into recovery.ravendump
        Voron.Recovery c:\ravendb\databases\Northwind\ c:\ravendb\recovery\northwind\

Options:
    -OutputFileName <file name> 
        Will overwrite the default file name with your own file name (under output directory).
    -PageSizeInKB <page size>
        Will set the recovery tool to work with page sizes other than 4kb.
    -TableValueEntries <entries count>
        Will set the recovery tool to validate the table value with a entry count other than 5.
    -InitialContextSizeInMB <context size>
        Will set the recovery tool to use a context of the provided size in MB.
    -InitialContextLongLivedSizeInKB <long lived size>
        Will set the recovery tool to use a long lived context size of the provided size in KB.
    -RefreshRateInSeconds <refresh rate>
        Will set the recovery tool refresh to console rate interval in seconds.
"

);
        }
    }
}
